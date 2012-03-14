module Network.Server.MultiLevelDB.Config (
    argspec, Config(..),
    ) where

import System.Console.CmdArgs

data Config = Config {
    dbPath :: String,
    port   :: Int
} deriving (Show, Data, Typeable, Eq)

argspec = Config {
    dbPath  = "/tmp/multileveldb" &= typ "DIR" &= help "Data directory for the database",
    port    = 4455 &= typ "PORT" &= help "Port to listen on"
} &= summary "MultiLevelDB"
