import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString)

import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType


data Request = Request MultiLevelDBWireType B.ByteString

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

handleRequest :: Request -> IO Builder

handleRequest (Request MULTI_LEVELDB_GET raw) = do
    return $ copyByteString "OK GET\r\n"

handleRequest (Request MULTI_LEVELDB_PUT raw) = do
    return $ copyByteString "OK PUT\r\n"

main = do
    runServer pipe 4455
    where
        pipe = RequestPipeline parseRequest handleRequest 10
