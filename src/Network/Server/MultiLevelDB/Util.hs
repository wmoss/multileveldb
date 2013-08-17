module Network.Server.MultiLevelDB.Util where


import Text.ProtocolBuffers.WireMessage (messageGet, messagePut)

import GHC.Conc.Sync (TVar, atomically, readTVar, writeTVar)

import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.ByteString.Char8 as S
import Data.Binary.Put (runPut, putWord32le, putWord8)
import Data.Binary.Get (runGet, getWord32le)


decodeProto raw = case messageGet raw of
    Right x -> fst x
    Left e -> error "Failed to decode proto"

lTos = S.concat . B.toChunks
sTol = B.fromChunks . (:[])

applyTVar :: TVar a -> (a -> a) -> IO a
applyTVar tv f = atomically $ do
    v <- readTVar tv
    writeTVar tv $ f v
    return v

readAndIncr :: TVar Integer -> IO Integer
readAndIncr = flip applyTVar $ (+ 1)

readAndIncrBy :: TVar Integer -> Integer -> IO Integer
readAndIncrBy tv i = applyTVar tv (+ i)

put2Words a b = lTos $ runPut $ do
    putWord8 a
    putWord8 b

integerToWord32 = lTos . runPut . putWord32le . fromIntegral
word32ToInteger = toInteger . runGet getWord32le . sTol


makeKey prefix index = lTos $ runPut $ do
    putWord8 prefix
    putWord32le $ fromIntegral index

makePrimaryKey = makeKey 1
makeIndexKey = makeKey 2
