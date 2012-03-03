import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString, copyLazyByteString)

import GHC.Conc.Sync (TVar, atomically, newTVarIO, readTVar, writeTVar)
import Data.Binary.Put (runPut, putWord32le, putWord8)
import Data.Binary.Get (runGet)
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
    B.concat [ bcode, blen, raw ]
    where
        blen = runPut $ putWord32le $ fromIntegral $ B.length raw
        bcode = runPut $ putWord32le $ fromIntegral $ fromEnum code

makeQueryResponse = makeResponse MULTI_LEVELDB_QUERY_RESP . messagePut . Query.QueryResponse

decodeProto raw = case messageGet raw of
    Right x -> fst x
    Left e -> error "Failed to decode proto"

lTos = S.concat . B.toChunks
sTol = B.fromChunks . (:[])

getIndex :: TVar Integer -> IO Integer
getIndex incr = atomically $ do
    v <- readTVar incr
    writeTVar incr $ v + 1
    return v

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
    key <- fmap (lTos . runPut) $ do
        return $ putWord8 1
        fmap (putWord32le . fromIntegral) $ getIndex incr
    put db [ ] key $ (lTos $ Put.value obj)
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
    incr <- newTVarIO 0
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        runServer (pipe db incr) 4455
    where
        pipe db incr = RequestPipeline parseRequest (handleRequest db incr) 10
