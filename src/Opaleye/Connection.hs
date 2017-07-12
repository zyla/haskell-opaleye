-- | Abstraction layer around @postgresql-simple@ Connection.

module Opaleye.Connection (IsConnection(..)) where

import           Data.Int (Int64)
import qualified Database.PostgreSQL.Simple as PGS
import qualified Database.PostgreSQL.Simple.FromRow as PGS

class IsConnection conn where
  execute_ :: conn -> PGS.Query -> IO Int64
  queryWith_ :: PGS.RowParser r -> conn -> PGS.Query -> IO [r]
  foldWith_ :: PGS.RowParser r -> conn -> PGS.Query -> a -> (a -> r -> IO a) -> IO a

instance IsConnection PGS.Connection where
  execute_ = PGS.execute_
  queryWith_ = PGS.queryWith_
  foldWith_ = PGS.foldWith_
