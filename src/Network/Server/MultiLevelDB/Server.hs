import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString)

import Text.ProtocolBuffers.WireMessage (messageGet)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put


data Request = Request MultiLevelDBWireType B.ByteString

decodeProto raw = case messageGet raw of
    Right x -> fst x
    Left e -> error "Failed to decode proto"

lTos = S.concat . B.toChunks

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

handleRequest :: Request -> IO Builder

handleRequest (Request MULTI_LEVELDB_GET raw) = do
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        res <- get db [ ] $ lTos $ Get.key obj
        case res of
            Just v -> return $ copyByteString $ S.concat [v, "\r\n"]
            Nothing -> return $ copyByteString "MISSING\r\n"
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest (Request MULTI_LEVELDB_PUT raw) = do
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        put db [ ] (lTos $ Put.key obj) (lTos $ Put.value obj)
        return $ copyByteString "OK\r\n"
    where
        obj = decodeProto raw :: Put.PutRequest

main = do
    runServer pipe 4455
    where
        pipe = RequestPipeline parseRequest handleRequest 10
