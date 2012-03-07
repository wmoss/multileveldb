import Database.LevelDB
import Network.Server.ScalableServer

import qualified Data.Attoparsec as Atto
import qualified Data.Attoparsec.Binary as AttoB
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Blaze.ByteString.Builder (Builder)
import Blaze.ByteString.Builder.ByteString (copyByteString, copyLazyByteString)

import GHC.Conc.Sync (TVar, atomically, newTVarIO, readTVar, writeTVar, readTVarIO)
import Data.Binary.Put (runPut, putWord32le, putWord8, putLazyByteString)
import Data.Binary.Get (runGet, getWord32le)
import qualified Data.Sequence as Seq
import GHC.Word (Word8)
import qualified Data.UString as US

import Data.Maybe (fromJust)
import Data.Bson (Document, Field(..), Value(..), Binary(..))
import Data.Bson.Binary (getDocument, putDocument)
import qualified Data.Map as M

import Text.ProtocolBuffers.WireMessage (messageGet, messagePut)
import Network.Server.MultiLevelDB.Proto.Request.MultiLevelDBWireType
import Network.Server.MultiLevelDB.Proto.Request.GetRequest as Get
import Network.Server.MultiLevelDB.Proto.Request.PutRequest as Put
import Network.Server.MultiLevelDB.Proto.Request.ScanRequest as Scan
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

loadIndex :: S.ByteString -> (Integer -> S.ByteString) -> DB ->
             IO (TVar Integer)
loadIndex startKey integerToKey db = do
    last <- get db [ ] startKey
    x <- case last of
        Just v -> checkNext $ word32ToInteger v
        Nothing -> return 0
    newTVarIO $ x + 1
    where
        checkNext :: Integer -> IO Integer
        checkNext index = do
            res <- get db [ ] $ integerToKey $ index + 1
            case res of
                Just v -> checkNext $ index + 1
                Nothing -> return index

loadPrimaryIndex = loadIndex lastPrimaryKey makePrimaryKey
loadIndexIndex = loadIndex lastIndexKey makeIndexKey

loadIndexes :: DB -> TVar Integer -> IO (TVar (M.Map S.ByteString Integer))
loadIndexes db tvindex = do
    index <- readTVarIO tvindex
    set <- fmap (M.fromList . catMaybes) $ loadIndex index
    newTVarIO set
    where
        loadIndex index = do
            if index /= 0
              then do
                field <- get db [ ] $ S.snoc (makeIndexKey index) '\NUL'
                fmap (fmap (,index) field :) $ loadIndex $ index - 1
              else return []

readAndIncr :: TVar Integer -> IO Integer
readAndIncr = flip applyTVar $ (+ 1)

put2Words a b = lTos $ runPut $ do
    putWord8 a
    putWord8 b

lastPrimaryKey = put2Words 0 1
lastIndexKey = put2Words 0 2

integerToWord32 = lTos . runPut . putWord32le . fromIntegral
word32ToInteger = toInteger . runGet getWord32le . sTol

applyTVar :: TVar a -> (a -> a) -> IO a
applyTVar tv f = atomically $ do
    v <- readTVar tv
    writeTVar tv $ f v
    return v

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
        Just v -> return $ copyLazyByteString $ makeQueryResponse $ Seq.singleton $ sTol v
        Nothing -> return $ copyLazyByteString $ makeQueryResponse $ Seq.empty
    where
        obj = decodeProto raw :: Get.GetRequest

handleRequest db incr _ _ (Request MULTI_LEVELDB_PUT raw) = do
    index <- readAndIncr incr
    let key = makePrimaryKey index
    write db [ ] [ Put key $ lTos $ Put.value obj
                 , Put lastPrimaryKey $ integerToWord32 index]
    return $ copyByteString $ S.concat ["OK ", key, "\r\n"]
    where
        obj = decodeProto raw :: Put.PutRequest
        doc = runGet getDocument $ Put.value obj


handleRequest db _ _ _ (Request MULTI_LEVELDB_SCAN raw) = do
    case runGet getDocument $ Scan.query obj of
        [field] -> do
            withIterator db [ ] $ \iter -> do
                iterFirst iter
                res <- scan field iter
                return $ copyLazyByteString $ makeQueryResponse $ Seq.fromList $ map (runPut . putDocument) res
        otherwise -> error "Currently, multifield queries are not supported"
    where
        obj = decodeProto raw :: Scan.ScanRequest

        scan :: Field -> Iterator -> IO [Document]
        scan field iter = do
            valid <- iterValid iter
            case valid of
                False -> return []
                True -> do
                    key <- iterKey iter
                    case S.head key == keyPrefix of
                        False -> do
                            _ <- iterNext iter
                            scan field iter
                        True -> do
                            val <- iterValue iter
                            _   <- iterNext iter
                            let d = runGet getDocument $ B.fromChunks [val]
                            case any (== field) d of
                                True -> fmap (d :) $ scan field iter
                                False -> scan field iter


-- TODO: Index all the existing records
handleRequest db _ iincr indexes (Request MULTI_LEVELDB_INDEX raw) = do
    res <- get db [ ] key
    case res of
        Just _ -> return $ copyByteString "INDEX ALREADY EXISTS\r\n"
        Nothing -> do
            index <- readAndIncr iincr
            write db [ ] [ Put key $ integerToWord32 index
                         , Put (S.snoc (makeIndexKey index) '\NUL') field
                         , Put lastIndexKey $ integerToWord32 index]
            applyTVar indexes $ M.insert field index
            return $ copyByteString "OK\r\n"
    where
        field = case runGet getDocument $ Index.field obj of
            [field] -> US.toByteString $ label field
            otherwise -> error "Multi-field indexes are not supported"
        key = S.cons indexPrefix $ S.snoc field '\NUL'
        obj = decodeProto raw :: Index.AddIndex

handleRequest db _ iincr _ (Request MULTI_LEVELDB_DUMP _) = do
    withIterator db [ ] $ \iter -> do
        iterFirst iter
        fmap (copyLazyByteString . makeQueryResponse . Seq.fromList) $ dump iter
    where
        dump iter = do
            valid <- iterValid iter
            case valid of
                True -> do
                    key <- iterKey iter
                    val <- iterValue iter
                    _   <- iterNext iter
                    let doc = runPut $ putDocument $ ["key" := (Bin $ Binary key),
                                                      "value" := (Bin $ Binary val)]
                    fmap (doc :) $ dump iter
                False -> return []

main = do
    withLevelDB "/tmp/leveltest" [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        incr <- loadPrimaryIndex db
        iincr <- loadIndexIndex db
        indexes <- loadIndexes db iincr
        runServer (pipe db incr iincr indexes) 4455
    where
        pipe db incr iincr indexes = RequestPipeline parseRequest (handleRequest db incr iincr indexes) 10
