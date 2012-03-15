module Network.Server.MultiLevelDB.Request (
    parseRequest, handleRequest
    ) where

import Network.Server.MultiLevelDB.Util
import Network.Server.MultiLevelDB.Const

import Database.LevelDB

import Control.Applicative
import Control.Monad (mapM)

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

import GHC.Conc.Sync (TVar, readTVarIO)

import Text.ProtocolBuffers.WireMessage (messagePut)
import Text.ProtocolBuffers.Basic (uFromString)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put
import Network.Server.MultiLevelDB.Proto.Request.ScanRequest as Scan
import Network.Server.MultiLevelDB.Proto.Request.AddIndex as Index
import Network.Server.MultiLevelDB.Proto.Request.LookupRequest as Lookup
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

makeQueryResponse = copyLazyByteString . makeResponse MULTI_LEVELDB_QUERY_RESP . messagePut . Query.QueryResponse
makeStatusResponse s r = copyLazyByteString $ makeResponse MULTI_LEVELDB_STATUS_RESP $ messagePut $ Status.StatusResponse (Just s) r

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

handleRequest :: DB -> TVar Integer -> TVar Integer -> TVar (M.Map S.ByteString Integer) -> Request -> IO Builder

handleRequest db _ _ _ (Request MULTI_LEVELDB_GET raw) = do
    res <- get db [ ] $ lTos $ Get.key obj
    case res of
        Just v -> return $ makeQueryResponse $ Seq.singleton $ sTol v
        Nothing -> return $ makeQueryResponse $ Seq.empty
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest db incr _ tvindexes (Request MULTI_LEVELDB_PUT raw) = do
    case MP.unpack obj of
        MP.ObjectMap objs -> do
            index <- readAndIncr incr
            let key = makePrimaryKey index

            indexes <- readTVarIO tvindexes
            let indexTuple (MP.ObjectRAW k, v) = (, lTos $ MP.pack v) <$> (M.lookup k indexes)
            let makePut (iindex, bsfield) = flip Put zero $ S.concat [makeIndexKey iindex, bsfield, integerToWord32 index]
            let indexPuts = map makePut $ catMaybes $ map indexTuple objs
            write db [ ] $ [ Put key $ lTos obj
                           , Put lastPrimaryKey $ integerToWord32 index] ++
              indexPuts

            return $ copyLazyByteString $ makeResponse MULTI_LEVELDB_PUT_RESP $ messagePut $ PutResponse.PutResponse (sTol key) obj

        otherwise -> error "Stored object must be a map type"
    where
        obj = Put.value $ decodeProto raw
        zero = lTos $ runPut $ putWord8 0

handleRequest db _ _ _ (Request MULTI_LEVELDB_SCAN raw) = do
    case obj of
        MP.ObjectMap [(k, v)] -> do
            withIterator db [ ] $ \iter -> do
                iterFirst iter

                docs <- iterItems iter
                return $ makeResp $ filterDocs docs
                where
                    makeResp = makeQueryResponse . Seq.fromList . map (sTol . snd)
                    filterDocs = filter filterDoc . filter filterPrefix
                    filterPrefix = (==) keyPrefix . S.head . fst
                    filterDoc = fromMaybe False . fmap (== v) . M.lookup k . MP.unpack . snd

        otherwise -> error "Currently, multifield queries are not supported"
    where
        obj = MP.unpack $ Scan.query $ decodeProto raw

-- TODO: Index all the existing records
handleRequest db _ iincr indexes (Request MULTI_LEVELDB_INDEX raw) = do
    res <- get db [ ] key
    case res of
        Just _ -> return $ makeStatusResponse StatusTypes.FAILED $ Just $ uFromString "Index already exists"
        Nothing -> do
            index <- readAndIncr iincr
            write db [ ] [ Put key $ integerToWord32 index
                         , Put (S.snoc (makeIndexKey index) '\NUL') field
                         , Put lastIndexKey $ integerToWord32 index]
            applyTVar indexes $ M.insert field index
            return $ makeStatusResponse StatusTypes.OKAY Nothing
    where
        field = case MP.unpack $  Index.field obj of
            [field] -> field
            otherwise -> error "Multi-field indexes are not supported"
        key = S.cons indexPrefix $ S.snoc field '\NUL'
        obj = decodeProto raw :: Index.AddIndex

handleRequest db _ iincr _ (Request MULTI_LEVELDB_DUMP _) = do
    withIterator db [ ] $ \iter -> do
        iterFirst iter
        fmap (makeQueryResponse . Seq.fromList . map MP.pack) $ iterItems iter
    where

handleRequest db _ _ tvindexes (Request MULTI_LEVELDB_LOOKUP raw) = do
    case obj of
        MP.ObjectMap [(MP.ObjectRAW k, v)] -> do
            indexes <- readTVarIO tvindexes
            case M.lookup k indexes of
                Just index -> do
                    withIterator db [ ] $ \iter -> do
                        let bsfield = lTos $ MP.pack v
                        _ <- iterSeek iter $ S.concat [makeIndexKey index, bsfield]
                        docs <- map (S.cons keyPrefix . getPrimaryKey) . takeWhile (equalsField bsfield) <$> iterKeys iter >>= mapM (get db [ ])
                        return $ makeQueryResponse $ Seq.fromList $ map sTol $ catMaybes docs
                Nothing -> error "Field not indexed"
        otherwise -> error "Only single index queries are currently supported"
    where
        obj = MP.unpack $ Lookup.query $ decodeProto raw :: MP.Object
        getPrimaryKey s = S.drop (S.length s - 4) s
        equalsField f k = f == (S.take (S.length k - 9) $ S.drop 5 k)
