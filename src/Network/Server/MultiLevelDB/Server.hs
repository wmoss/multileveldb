import Database.LevelDB
import Network.Server.ScalableServer

import Network.Server.MultiLevelDB.Util
import Network.Server.MultiLevelDB.Const
import Network.Server.MultiLevelDB.Config
import Network.Server.MultiLevelDB.Request

import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S

import GHC.Conc.Sync (TVar, readTVarIO, newTVarIO)
import qualified Data.Sequence as Seq
import GHC.Word (Word8)

import Control.Applicative
import Control.Concurrent (myThreadId)
import Control.Monad (mapM)
import Data.Maybe (fromJust, catMaybes, fromMaybe)

import qualified Data.Map as M

import System.Console.CmdArgs (cmdArgs)

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

main = do
    cfg <- cmdArgs argspec

    withLevelDB (dbPath cfg) [ CreateIfMissing, CacheSize 2048 ] $ \db -> do
        incr <- loadPrimaryIndex db
        iincr <- loadIndexIndex db
        indexes <- loadIndexes db iincr
        threadId <- myThreadId
        let requestHandler = handleRequest $ RequestState db incr iincr indexes threadId
        runServer (RequestPipeline parseRequest requestHandler 10) $ fromIntegral $ port cfg
