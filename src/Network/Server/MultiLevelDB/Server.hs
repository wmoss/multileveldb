import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString, copyLazyByteString)
import Data.Maybe (fromMaybe)

import GHC.Conc.Sync (TVar, atomically, newTVarIO, readTVar, writeTVar)
import Data.Binary.Put (runPut, putWord32le, putWord8, putLazyByteString)
import Data.Binary.Get (runGet, getWord32le)
import qualified Data.Sequence as Seq

import Data.Bson (Document, Field)
import Data.Bson.Binary (getDocument, putDocument)

import Text.ProtocolBuffers.WireMessage (messageGet, messagePut)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put
import Network.Server.MultiLevelDB.Proto.Request.LookupRequest as Lookup
import Network.Server.MultiLevelDB.Proto.Request.QueryResponse as Query


data Request = Request MultiLevelDBWireType B.ByteString

makeResponse code raw =
    runPut $ do
        putWord32le $ fromIntegral $ fromEnum code
        putWord32le $ fromIntegral $ B.length raw
        putLazyByteString raw

makeQueryResponse = makeResponse MULTI_LEVELDB_QUERY_RESP . messagePut . Query.QueryResponse

decodeProto raw = case messageGet raw of
    Right x -> fst x
    Left e -> error "Failed to decode proto"

lTos = S.concat . B.toChunks
sTol = B.fromChunks . (:[])

keyPrefix = B.head $ runPut $ putWord8 1

makeKey :: Integer -> S.ByteString
makeKey index = lTos $ runPut $ do
    putWord8 1
    putWord32le $ fromIntegral index

loadIndex :: DB -> IO (TVar Integer)
loadIndex db = do
    last <- get db [ ] lastIndexKey
    x <- case last of
        Just v -> do
            withIterator db [ ] $ \iter -> do
              _ <- iterSeek iter $ S.cons keyPrefix v
              fmap (toInteger . runGet getWord32le . sTol . S.tail) $ checkNext iter
        Nothing -> return 0
    newTVarIO $ x + 1
    where
        -- Since we don't have transaction support on LevelDB we have to
        -- search ahead to make sure the value we wrote isn't stale
        checkNext :: Iterator -> IO S.ByteString
        checkNext iter = do
            index <- iterKey iter
            _ <- iterNext iter
            valid <- iterValid iter
            case valid of
                 True -> do
                     new <- iterKey iter
                     if S.head new == keyPrefix
                       then checkNext iter
                       else return index
                 False -> return index

getIndex :: TVar Integer -> IO Integer
getIndex incr = atomically $ do
    v <- readTVar incr
    writeTVar incr $ v + 1
    return v

lastIndexKey = lTos $ runPut $ do
    putWord8 0
    putWord8 0

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

handleRequest :: DB -> TVar Integer -> Request -> IO Builder

handleRequest db _ (Request MULTI_LEVELDB_GET raw) = do
    res <- get db [ ] $ lTos $ Get.key obj
    case res of
        Just v -> return $ copyLazyByteString $ makeQueryResponse $ Seq.singleton $ sTol v
        Nothing -> return $ copyByteString "MISSING\r\n"
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest db incr (Request MULTI_LEVELDB_PUT raw) = do
    index <- getIndex incr
    let key = makeKey index
    write db [ ] [ Put key $ lTos $ Put.value obj
                 , Put lastIndexKey $ lTos $ runPut $ putWord32le $ fromIntegral index]
    return $ copyByteString $ S.concat ["OK ", key, "\r\n"]
    where
        obj = decodeProto raw :: Put.PutRequest

handleRequest db incr (Request MULTI_LEVELDB_LOOKUP raw) = do
    case runGet getDocument $ Lookup.query obj of
        [field] -> do
            withIterator db [ ] $ \iter -> do
                iterFirst iter
                res <- lookup field iter []
                return $ copyLazyByteString $ makeQueryResponse $ Seq.fromList $ map (runPut . putDocument) res
        otherwise -> error "Currently, multifield queries are not supported"
    where
        obj = decodeProto raw :: Lookup.LookupRequest

        lookup :: Field -> Iterator -> [Document] -> IO [Document]
        lookup field iter res = do
            valid <- iterValid iter
            case valid of
                True -> do
                    val <- iterValue iter
                    _   <- iterNext iter
                    let d = runGet getDocument $ B.fromChunks [val]
                    case any (== field) d of
                        True -> lookup field iter $ d : res
                        False -> lookup field iter res
                False -> return res

main = do
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        incr <- loadIndex db
        runServer (pipe db incr) 4455
    where
        pipe db incr = RequestPipeline parseRequest (handleRequest db incr) 10
