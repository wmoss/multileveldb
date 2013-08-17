module Network.Server.MultiLevelDB.Request (
    RequestState(..), parseRequest, handleRequest
    ) where

import Network.Server.MultiLevelDB.Util
import Network.Server.MultiLevelDB.Const

import Database.LevelDB

import Control.Applicative
import Control.Monad (mapM)
import Control.Exception (handle, SomeException)

import Data.Binary.Put (runPut, putWord32le, putWord8, putLazyByteString)
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyLazyByteString)

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import qualified Data.Sequence as Seq
import qualified Data.Map as M
import qualified Data.MessagePack.Unpack as MP
import qualified Data.MessagePack.Pack as MP
import qualified Data.MessagePack.Object as MP
import Data.Maybe (catMaybes, fromMaybe)
import Data.Monoid (mappend, mempty)

import GHC.Conc.Sync (TVar, readTVarIO)

import Text.ProtocolBuffers.WireMessage (messagePut)
import Text.ProtocolBuffers.Basic (uFromString)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put
import Network.Server.MultiLevelDB.Proto.Request.ScanRequest as Scan
import Network.Server.MultiLevelDB.Proto.Request.AddIndex as Index
import Network.Server.MultiLevelDB.Proto.Request.LookupRequest as Lookup
import Network.Server.MultiLevelDB.Proto.Request.DeleteRequest as Delete
import Network.Server.MultiLevelDB.Proto.Request.QueryResponse as Query
import Network.Server.MultiLevelDB.Proto.Request.StatusResponse as Status
import Network.Server.MultiLevelDB.Proto.Request.PutResponse as PutResponse
import Network.Server.MultiLevelDB.Proto.Request.StatusResponse.Status as StatusTypes

data Request = Request MultiLevelDBWireType B.ByteString


makeResponse code raw =
    runPut $ do
        putWord32le $ fromIntegral $ fromEnum code
        putWord32le $ fromIntegral $ B.length raw
        putLazyByteString raw

queryResponseChunkSize = 100
makeQueryResponse s = makeQueryResponse' 0 s
  where
      total = Seq.length s
      makeQueryResponse' :: Int -> Seq.Seq B.ByteString -> Builder
      makeQueryResponse' o s
          | Seq.null s = mempty
          | otherwise = mappend nextChunk $ makeQueryResponse' (o + queryResponseChunkSize) $ Seq.drop queryResponseChunkSize s
              where
                  nextChunk = copyLazyByteString $ makeResponse MULTI_LEVELDB_QUERY_RESP $ messagePut $ Query.QueryResponse { results = Seq.take queryResponseChunkSize s,
                                                                                                                              Query.offset = fromIntegral o,
                                                                                                                              total = fromIntegral total }

makeStatusResponse s r = copyLazyByteString $ makeResponse MULTI_LEVELDB_STATUS_RESP $ messagePut $ Status.StatusResponse (Just s) r

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

data RequestState = RequestState {
    db            :: DB,
    tvKeyIndex    :: TVar Integer,
    tvIndexIndex  :: TVar Integer,
    tvIndexes     :: TVar (M.Map S.ByteString Integer)
    }

handleRequest :: RequestState -> Request -> IO Builder
handleRequest state pb = handle errorHandler $ handleRequest' state pb
    where
        errorHandler e = return $ makeStatusResponse StatusTypes.FAILED $ Just $ uFromString $ show (e :: SomeException)

handleRequest' :: RequestState -> Request -> IO Builder

handleRequest' state (Request MULTI_LEVELDB_GET raw) = do
    res <- get (db state) [ ] $ lTos $ Get.key obj
    case res of
        Just v -> return $ makeQueryResponse $ Seq.singleton $ sTol v
        Nothing -> return $ makeQueryResponse $ Seq.empty
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest' state (Request MULTI_LEVELDB_PUT raw) = do
    case MP.unpack obj of
        MP.ObjectMap objs -> do
            index <- readAndIncr $ tvKeyIndex state
            let key = makePrimaryKey index

            indexes <- readTVarIO $ tvIndexes state
            let indexTuple (MP.ObjectRAW k, v) = (, lTos $ MP.pack v) <$> (M.lookup k indexes)
            let makePut (iindex, bsfield) = flip Put zero $ S.concat [makeIndexKey iindex, bsfield, integerToWord32 index]
            let indexPuts = map makePut $ catMaybes $ map indexTuple objs
            write (db state) [ ] $ [ Put key $ lTos obj
                                   , Put lastPrimaryKey $ integerToWord32 index] ++
              indexPuts

            return $ copyLazyByteString $ makeResponse MULTI_LEVELDB_PUT_RESP $ messagePut $ PutResponse.PutResponse (sTol key) obj

        otherwise -> error "Stored object must be a map type"
    where
        obj = Put.value $ decodeProto raw
        zero = lTos $ runPut $ putWord8 0

-- TODO: Index all the existing records
handleRequest' state (Request MULTI_LEVELDB_INDEX raw) = do
    let levelDB = db state
    res <- get levelDB [ ] key
    case res of
        Just _ -> return $ makeStatusResponse StatusTypes.FAILED $ Just $ uFromString "Index already exists"
        Nothing -> do
            index <- readAndIncr $ tvIndexIndex state
            write levelDB [ ] [ Put key $ integerToWord32 index
                              , Put (S.snoc (makeIndexKey index) '\NUL') field
                              , Put lastIndexKey $ integerToWord32 index]
            applyTVar (tvIndexes state) $ M.insert field index
            return $ makeStatusResponse StatusTypes.OKAY Nothing
    where
        field = case MP.unpack $  Index.field obj of
            [field] -> field
            otherwise -> error "Multi-field indexes are not supported"
        key = S.cons indexPrefix $ S.snoc field '\NUL'
        obj = decodeProto raw :: Index.AddIndex

handleRequest' state (Request MULTI_LEVELDB_DUMP _) = do
    withIterator (db state) [ ] $ \iter -> do
        iterFirst iter
        fmap (makeQueryResponse . Seq.fromList . map MP.pack) $ iterItems iter
    where

handleRequest' state (Request MULTI_LEVELDB_LOOKUP raw) = do
    case obj of
        MP.ObjectMap [(MP.ObjectRAW k, v)] -> do
            indexes <- readTVarIO $ tvIndexes state
            makeResp <$> case M.lookup k indexes of
                Just index -> fmap catMaybes $ map getPrimaryKeyFromIndex <$> scanIndex levelDB index (k, v) >>= mapM (get levelDB [ ])
                Nothing -> case fromMaybe False $ Lookup.allow_scan pb of
                    False -> error "Field not indexed"
                    True -> map snd <$> lookupScan levelDB (k, v)
        otherwise -> error "Only single index queries are currently supported"
    where
        levelDB = db state
        pb = decodeProto raw
        obj = MP.unpack $ Lookup.query pb :: MP.Object

        limit = fromIntegral $ fromMaybe 0 $ Lookup.limit pb
        offset = fromIntegral $ fromMaybe 0 $ Lookup.offset pb

        makeResp = makeQueryResponse . Seq.take limit . Seq.drop offset . Seq.fromList . map sTol

-- TODO : Put the deleted indexes on the free list
handleRequest' state (Request MULTI_LEVELDB_DELETE raw) = do
    case obj of
        MP.ObjectMap [(MP.ObjectRAW k, v)] -> do
            indexes <- readTVarIO $ tvIndexes state
            write (db state) [ ] =<< case M.lookup k indexes of
                Just index -> do
                    indexKeys <- scanIndex levelDB index (k, v)
                    let keys = map getPrimaryKeyFromIndex indexKeys
                    return $ map Del $ indexKeys ++ keys
                Nothing -> do
                    map (Del . fst) <$> lookupScan levelDB (k, v)
            return $ makeStatusResponse StatusTypes.OKAY Nothing
        otherwise -> error "Only single index queries are currently supported"
    where
        levelDB = db state
        obj = MP.unpack $ Delete.query $ decodeProto raw :: MP.Object

handleRequest' _ _ = error "Unknown command"

lookupScan levelDB (k, v) = do
    withIterator levelDB [ ] $ \iter -> do
        iterFirst iter
        filterDocs <$> iterItems iter
    where
        filterDocs = filter filterDoc . filter filterPrefix
        filterPrefix = (==) keyPrefix . S.head . fst
        filterDoc = fromMaybe False . fmap (== v) . M.lookup k . MP.unpack . snd

scanIndex levelDB index (k, v) = do
    withIterator levelDB [ ] $ \iter -> do
        let bsfield = lTos $ MP.pack v
        _ <- iterSeek iter $ S.concat [makeIndexKey index, bsfield]
        takeWhile (equalsField bsfield) <$> iterKeys iter
    where
        equalsField f k = f == (S.take (S.length k - 9) $ S.drop 5 k)

getPrimaryKeyFromIndex = S.cons keyPrefix . getPrimaryKey
    where
        getPrimaryKey s = S.drop (S.length s - 4) s
