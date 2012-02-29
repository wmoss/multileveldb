import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString)

import GHC.Conc.Sync (TVar, atomically, newTVarIO, readTVar, writeTVar)
import Data.Binary.Put (runPut, putWord32le)

import Text.ProtocolBuffers.WireMessage (messageGet)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put


data Request = Request MultiLevelDBWireType B.ByteString

decodeProto raw = case messageGet raw of
    Right x -> fst x
    Left e -> error "Failed to decode proto"

lTos = S.concat . B.toChunks

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
        Just v -> return $ copyByteString $ S.concat [v, "\r\n"]
        Nothing -> return $ copyByteString "MISSING\r\n"
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest db incr (Request MULTI_LEVELDB_PUT raw) = do
    key <- fmap (lTos . runPut . putWord32le . fromIntegral) $ getIndex incr
    put db [ ] key (lTos $ Put.value obj)
    return $ copyByteString $ S.concat ["OK ", key, "\r\n"]
    where
        obj = decodeProto raw :: Put.PutRequest

main = do
    incr <- newTVarIO 0
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        runServer (pipe db incr) 4455
    where
        pipe db incr = RequestPipeline parseRequest (handleRequest db incr) 10
