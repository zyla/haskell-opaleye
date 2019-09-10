module Opaleye.Locking
  ( module Opaleye.Locking
  , PQ.LockStrength(..)
  ) where

import qualified Opaleye.Select as S
import qualified Opaleye.Internal.QueryArr as Q
import qualified Opaleye.Internal.PrimQuery as PQ

{-| Lock the rows returned by the query, with the specified lock strength.

This adds a SQL locking clause (@FOR UPDATE@, @FOR NO KEY UPDATE@, @FOR SHARE@, @FOR KEY SHARE@) to the query.

'locking' can't be used on aggregation queries.

There are more caveats. For details please consult <https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE>.

-}
locking :: PQ.LockStrength -> S.SelectArr a b -> S.SelectArr a b
locking s q =
  Q.simpleQueryArr ((\(x, pq, t) -> (x, PQ.Locking s pq, t)) . Q.runSimpleQueryArr q)
