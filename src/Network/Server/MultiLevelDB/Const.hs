module Network.Server.MultiLevelDB.Const where

import Network.Server.MultiLevelDB.Util

import qualified Data.ByteString.Lazy.Char8 as B
import Data.Binary.Put (runPut, putWord8)


keyPrefix = B.head $ runPut $ putWord8 1
indexPrefix = B.head $ runPut $ putWord8 2
freePrefix = B.head $ runPut $ putWord8 3

lastPrimaryKey = put2Words 0 1
lastIndexKey = put2Words 0 2
lastFreeKey = put2Words 0 3
