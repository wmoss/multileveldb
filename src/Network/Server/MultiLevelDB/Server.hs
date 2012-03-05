import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString, copyLazyByteString)

import GHC.Conc.Sync (TVar, atomically, newTVarIO, readTVar, writeTVar)
import Data.Binary.Put (runPut, putWord32le, putWord8, putLazyByteString)
import Data.Binary.Get (runGet, getWord32le)
import qualified Data.Sequence as Seq
import GHC.Word (Word8)

import Data.Bson (Document, Field)
import Data.Bson.Binary (getDocument, putDocument)

import Text.ProtocolBuffers.WireMessage (messageGet, messagePut)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put
import Network.Server.MultiLevelDB.Proto.Request.LookupRequest as Lookup
import Network.Server.MultiLevelDB.Proto.Request.QueryResponse as Query
import Network.Server.MultiLevelDB.Proto.Request.AddIndex as Index


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
indexPrefix = B.head $ runPut $ putWord8 2

makeKey :: Word8 -> Integer -> S.ByteString
makeKey prefix index = lTos $ runPut $ do
    putWord8 prefix
    putWord32le $ fromIntegral index

makePrimaryKey = makeKey 1
makeIndexKey = makeKey 2

loadPrimaryIndex :: DB -> IO (TVar Integer)
loadPrimaryIndex db = do
    last <- get db [ ] lastPrimaryKey
    x <- case last of
        Just v -> checkNext $ keyToInteger v
        Nothing -> return 0
    newTVarIO $ x + 1
    where
        keyToInteger = toInteger . runGet getWord32le . sTol

        checkNext :: Integer -> IO Integer
        checkNext index = do
            res <- get db [ ] $ makePrimaryKey $ index + 1
            case res of
                Just v -> checkNext $ index + 1
                Nothing -> return index

readAndIncr :: TVar Integer -> IO Integer
readAndIncr tvi = atomically $ do
    v <- readTVar tvi
    writeTVar tvi $ v + 1
    return v

put2Words a b = lTos $ runPut $ do
    putWord8 a
    putWord8 b

lastPrimaryKey = put2Words 0 1
lastIndexKey = put2Words 0 2

integerToWord32 = lTos . runPut . putWord32le . fromIntegral

parseRequest :: Atto.Parser Request
parseRequest = do
    rid <- fmap (toEnum . fromIntegral) AttoB.anyWord32le
    size <- AttoB.anyWord32le
    raw <- Atto.take $ fromIntegral size
    return $ Request rid $ B.fromChunks [raw]

handleRequest :: DB -> TVar Integer -> TVar Integer -> Request -> IO Builder

handleRequest db _ _ (Request MULTI_LEVELDB_GET raw) = do
    res <- get db [ ] $ lTos $ Get.key obj
    case res of
        Just v -> return $ copyLazyByteString $ makeQueryResponse $ Seq.singleton $ sTol v
        Nothing -> return $ copyLazyByteString $ makeQueryResponse $ Seq.empty
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest db incr _ (Request MULTI_LEVELDB_PUT raw) = do
    index <- readAndIncr incr
    let key = makePrimaryKey index
    write db [ ] [ Put key $ lTos $ Put.value obj
                 , Put lastPrimaryKey $ integerToWord32 index]
    return $ copyByteString $ S.concat ["OK ", key, "\r\n"]
    where
        obj = decodeProto raw :: Put.PutRequest

handleRequest db _ _ (Request MULTI_LEVELDB_LOOKUP raw) = do
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
                False -> return res
                True -> do
                    key <- iterKey iter
                    case S.head key == keyPrefix of
                        False -> do
                            _ <- iterNext iter
                            lookup field iter res
                        True -> do
                            val <- iterValue iter
                            _   <- iterNext iter
                            let d = runGet getDocument $ B.fromChunks [val]
                            case any (== field) d of
                                True -> lookup field iter $ d : res
                                False -> lookup field iter res


-- TODO: Index all the existing records
handleRequest db _ iincr (Request MULTI_LEVELDB_INDEX raw) = do
    res <- get db [ ] key
    case res of
        Just _ -> return $ copyByteString "INDEX ALREADY EXISTS\r\n"
        Nothing -> do
            index <- readAndIncr iincr
            write db [ ] [ Put key $ integerToWord32 index
                         , Put (S.snoc (makeIndexKey index) '\NUL') field
                         , Put lastIndexKey $ integerToWord32 index]
            return $ copyByteString "OK\r\n"
    where
        field = case runGet getDocument $ Index.field obj of
            [field] -> US.toByteString $ label field
            otherwise -> error "Multi-field indexes are not supported"
        key = S.cons indexPrefix $ S.snoc field '\NUL'
        obj = decodeProto raw :: Index.AddIndex

main = do
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        incr <- loadPrimaryIndex db
        iincr <- newTVarIO 0
        runServer (pipe db incr iincr) 4455
    where
        pipe db incr iincr = RequestPipeline parseRequest (handleRequest db incr iincr) 10
