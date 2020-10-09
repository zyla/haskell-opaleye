{-# LANGUAGE LambdaCase #-}

module Opaleye.Internal.Sql where

import           Prelude hiding (product)

import qualified Opaleye.Internal.PrimQuery as PQ

import qualified Opaleye.Internal.HaskellDB.PrimQuery as HPQ
import           Opaleye.Internal.HaskellDB.PrimQuery (Symbol(Symbol))
import qualified Opaleye.Internal.HaskellDB.Sql as HSql
import qualified Opaleye.Internal.HaskellDB.Sql.Default as SD
import qualified Opaleye.Internal.HaskellDB.Sql.Print as SP
import qualified Opaleye.Internal.HaskellDB.Sql.Generate as SG
import qualified Opaleye.Internal.Tag as T

import qualified Data.List.NonEmpty as NEL
import qualified Data.Maybe as M
import qualified Data.Void as V

import qualified Control.Arrow as Arr

data Select = SelectFrom From
            | Table HSql.SqlTable
            | RelExpr HSql.SqlExpr
            -- ^ A relation-valued expression
            | SelectJoin Join
            | SelectValues Values
            | SelectBinary Binary
            | SelectLabel Label
            | SelectExists Exists
            deriving Show

data SelectAttrs =
    Star
  | SelectAttrs (NEL.NonEmpty (HSql.SqlExpr, Maybe HSql.SqlColumn))
  | SelectAttrsStar (NEL.NonEmpty (HSql.SqlExpr, Maybe HSql.SqlColumn))
  deriving Show

data From = From {
  attrs      :: SelectAttrs,
  tables     :: [(Lateral, Select)],
  criteria   :: [HSql.SqlExpr],
  groupBy    :: Maybe (NEL.NonEmpty HSql.SqlExpr),
  orderBy    :: [(HSql.SqlExpr, HSql.SqlOrder)],
  distinctOn :: Maybe (NEL.NonEmpty HSql.SqlExpr),
  limit      :: Maybe Int,
  offset     :: Maybe Int,
  for        :: Maybe LockStrength
  }
          deriving Show

data Join = Join {
  jJoinType   :: JoinType,
  jTables     :: (Select, Select),
  jCond       :: HSql.SqlExpr
  }
                deriving Show

data Values = Values {
  vAttrs  :: SelectAttrs,
  vValues :: [[HSql.SqlExpr]]
} deriving Show

data Binary = Binary {
  bOp :: BinOp,
  bSelect1 :: Select,
  bSelect2 :: Select
} deriving Show

data JoinType = LeftJoin | RightJoin | FullJoin deriving Show
data BinOp = Except | ExceptAll | Union | UnionAll | Intersect | IntersectAll deriving Show
data Lateral = Lateral | NonLateral deriving Show
data LockStrength = Update deriving Show

data Label = Label {
  lLabel  :: String,
  lSelect :: Select
} deriving Show

data Returning a = Returning a (NEL.NonEmpty HSql.SqlExpr)

data Exists = Exists
  { existsBool :: Bool
  , existsTable :: Select
  , existsCriteria :: Select
  } deriving Show

sqlQueryGenerator :: SG.SqlGenerator -> PQ.PrimQueryFold' V.Void Select
sqlQueryGenerator sqlGenerator = PQ.PrimQueryFold
  { PQ.unit              = unit
  , PQ.empty             = empty
  , PQ.baseTable         = baseTable sqlGenerator
  , PQ.product           = product sqlGenerator
  , PQ.aggregate         = aggregate sqlGenerator
  , PQ.distinctOnOrderBy = distinctOnOrderBy sqlGenerator
  , PQ.limit             = limit_
  , PQ.join              = join sqlGenerator
  , PQ.values            = values sqlGenerator
  , PQ.binary            = binary
  , PQ.label             = label
  , PQ.relExpr           = relExpr sqlGenerator
  , PQ.existsf           = exists
  , PQ.rebind            = rebind sqlGenerator
  , PQ.forUpdate         = forUpdate
  }

exists :: Bool -> Select -> Select -> Select
exists b q1 q2 = SelectExists (Exists b q1 q2)

sql :: SG.SqlGenerator -> ([HPQ.PrimExpr], PQ.PrimQuery' V.Void, T.Tag) -> Select
sql sqlGenerator (pes, pq, t) = SelectFrom $ newSelect { attrs = SelectAttrs (ensureColumns (makeAttrs pes))
                                          , tables = oneTable pqSelect }
  where pqSelect = PQ.foldPrimQuery (sqlQueryGenerator sqlGenerator) pq
        makeAttrs = flip (zipWith makeAttr) [1..]
        makeAttr pe i = sqlBinding sqlGenerator (Symbol ("result" ++ show (i :: Int)) t, pe)

unit :: Select
unit = SelectFrom newSelect { attrs  = SelectAttrs (ensureColumns []) }

empty :: V.Void -> select
empty = V.absurd

oneTable :: t -> [(Lateral, t)]
oneTable t = [(NonLateral, t)]

baseTable :: SG.SqlGenerator -> PQ.TableIdentifier -> [(Symbol, HPQ.PrimExpr)] -> Select
baseTable sqlGenerator ti columns = SelectFrom $
    newSelect { attrs = SelectAttrs (ensureColumns (map (sqlBinding sqlGenerator) columns))
              , tables = oneTable (Table (HSql.SqlTable (PQ.tiSchemaName ti) (PQ.tiTableName ti))) }

product :: SG.SqlGenerator -> NEL.NonEmpty (PQ.Lateral, Select) -> [HPQ.PrimExpr] -> Select
product sqlGenerator ss pes = SelectFrom $
    newSelect { tables = NEL.toList ss'
              , criteria = map (sqlExpr sqlGenerator) pes }
  where ss' = flip fmap ss $ Arr.first $ \case
          PQ.Lateral    -> Lateral
          PQ.NonLateral -> NonLateral

aggregate :: SG.SqlGenerator
          -> [(Symbol,
               (Maybe (HPQ.AggrOp, [HPQ.OrderExpr], HPQ.AggrDistinct),
                HPQ.Symbol))]
          -> Select
          -> Select
aggregate sqlGenerator aggrs' s =
  SelectFrom $ newSelect { attrs = SelectAttrs (ensureColumns (map attr aggrs))
                         , tables = oneTable s
                         , groupBy = (Just . groupBy') aggrs }
  where --- Although in the presence of an aggregation function,
        --- grouping by an empty list is equivalent to omitting group
        --- by, the equivalence does not hold in the absence of an
        --- aggregation function.  In the absence of an aggregation
        --- function, group by of an empty list will return a single
        --- row (if there are any and zero rows otherwise).  A query
        --- without group by will return all rows.  This is a weakness
        --- of SQL.  Really there ought to be a separate SELECT
        --- AGGREGATE operation.
        ---
        --- Syntactically one cannot group by an empty list in SQL.
        --- We take the conservative approach of explicitly grouping
        --- by a constant if we are provided with an empty list of
        --- group bys.  This yields a single group.  (Alternatively,
        --- we could check whether any aggregation functions have been
        --- applied and only group by a constant in the case where
        --- none have.  That would make the generated SQL less noisy.)
        ---
        --- "GROUP BY 0" means group by the zeroth column so we
        --- instead use an expression rather than a constant.
        handleEmpty :: [HSql.SqlExpr] -> NEL.NonEmpty HSql.SqlExpr
        handleEmpty =
          M.fromMaybe (return (SP.deliteral (HSql.ConstSqlExpr "0")))
          . NEL.nonEmpty

        aggrs = (map . Arr.second . Arr.second) HPQ.AttrExpr aggrs'

        groupBy' :: [(symbol, (Maybe aggrOp, HPQ.PrimExpr))]
                 -> NEL.NonEmpty HSql.SqlExpr
        groupBy' = handleEmpty
                   . map (sqlExpr sqlGenerator)
                   . map expr
                   . filter (M.isNothing . aggrOp)
        attr = sqlBinding sqlGenerator . Arr.second (uncurry aggrExpr)
        expr (_, (_, e)) = e
        aggrOp (_, (x, _)) = x


aggrExpr :: Maybe (HPQ.AggrOp, [HPQ.OrderExpr], HPQ.AggrDistinct) -> HPQ.PrimExpr -> HPQ.PrimExpr
aggrExpr = maybe id (\(op, ord, distinct) e -> HPQ.AggrExpr distinct op e ord)

distinctOnOrderBy :: SG.SqlGenerator -> Maybe (NEL.NonEmpty HPQ.PrimExpr) -> [HPQ.OrderExpr] -> Select -> Select
distinctOnOrderBy sqlGenerator distinctExprs orderExprs s = SelectFrom $ newSelect
    { tables     = oneTable s
    , distinctOn = fmap (sqlExpr sqlGenerator) <$> distinctExprs
    , orderBy    = map (SD.toSqlOrder sqlGenerator) $
        -- Postgres requires all 'DISTINCT ON' expressions to appear before any other
        -- 'ORDER BY' expressions if there are any.
        maybe [] (map (HPQ.OrderExpr ascOp) . NEL.toList) distinctExprs ++ orderExprs
    }
    where
        ascOp = HPQ.OrderOp
            { HPQ.orderDirection = HPQ.OpAsc
            , HPQ.orderNulls     = HPQ.NullsLast }

limit_ :: PQ.LimitOp -> Select -> Select
limit_ lo s = SelectFrom $ newSelect { tables = oneTable s
                                     , limit = limit'
                                     , offset = offset' }
  where (limit', offset') = case lo of
          PQ.LimitOp n         -> (Just n, Nothing)
          PQ.OffsetOp n        -> (Nothing, Just n)
          PQ.LimitOffsetOp l o -> (Just l, Just o)

join :: SG.SqlGenerator
     -> PQ.JoinType
     -> HPQ.PrimExpr
     -> PQ.Bindings HPQ.PrimExpr
     -> PQ.Bindings HPQ.PrimExpr
     -> Select
     -> Select
     -> Select
join sqlGenerator j cond pes1 pes2 s1 s2 =
  SelectJoin Join { jJoinType = joinType j
                  , jTables   = (selectFrom pes1 s1, selectFrom pes2 s2)
                  , jCond     = sqlExpr sqlGenerator cond }
  where selectFrom pes s = SelectFrom $ newSelect {
            attrs  = SelectAttrsStar (ensureColumns (map (sqlBinding sqlGenerator) pes))
          , tables = oneTable s
          }

-- Postgres seems to name columns of VALUES clauses "column1",
-- "column2", ... . I'm not sure to what extent it is customisable or
-- how robust it is to rely on this
values :: SG.SqlGenerator -> [Symbol] -> NEL.NonEmpty [HPQ.PrimExpr] -> Select
values sqlGenerator columns pes = SelectValues Values { vAttrs  = SelectAttrs (mkColumns columns)
                                         , vValues = NEL.toList ((fmap . map) (sqlExpr sqlGenerator) pes) }
  where mkColumns = ensureColumns . zipWith (flip (curry (sqlBinding sqlGenerator . Arr.second mkColumn))) [1..]
        mkColumn i = (HPQ.BaseTableAttrExpr . ("column" ++) . show) (i::Int)

binary :: PQ.BinOp -> (Select, Select) -> Select
binary op (select1, select2) = SelectBinary Binary {
  bOp = binOp op,
  bSelect1 = select1,
  bSelect2 = select2
  }

joinType :: PQ.JoinType -> JoinType
joinType PQ.LeftJoin = LeftJoin
joinType PQ.RightJoin = RightJoin
joinType PQ.FullJoin = FullJoin

binOp :: PQ.BinOp -> BinOp
binOp o = case o of
  PQ.Except       -> Except
  PQ.ExceptAll    -> ExceptAll
  PQ.Union        -> Union
  PQ.UnionAll     -> UnionAll
  PQ.Intersect    -> Intersect
  PQ.IntersectAll -> IntersectAll

newSelect :: From
newSelect = From {
  attrs      = Star,
  tables     = [],
  criteria   = [],
  groupBy    = Nothing,
  orderBy    = [],
  distinctOn = Nothing,
  limit      = Nothing,
  offset     = Nothing,
  for        = Nothing
  }

sqlExpr :: SG.SqlGenerator -> HPQ.PrimExpr -> HSql.SqlExpr
sqlExpr = SG.sqlExpr

sqlBinding :: SG.SqlGenerator -> (Symbol, HPQ.PrimExpr) -> (HSql.SqlExpr, Maybe HSql.SqlColumn)
sqlBinding sqlGenerator (Symbol sym t, pe) =
  (sqlExpr sqlGenerator pe, Just (HSql.SqlColumn (T.tagWith t sym)))

ensureColumns :: [(HSql.SqlExpr, Maybe a)]
             -> NEL.NonEmpty (HSql.SqlExpr, Maybe a)
ensureColumns = ensureColumnsGen (\x -> (x,Nothing))

-- | For ensuring that we have at least one column in a SELECT or RETURNING
ensureColumnsGen :: (HSql.SqlExpr -> a)
              -> [a]
              -> NEL.NonEmpty a
ensureColumnsGen f = M.fromMaybe (return . f $ HSql.ConstSqlExpr "0")
                   . NEL.nonEmpty

label :: String -> Select -> Select
label l s = SelectLabel (Label l s)

-- Very similar to 'baseTable'
relExpr :: SG.SqlGenerator -> HPQ.PrimExpr -> [(Symbol, HPQ.PrimExpr)] -> Select
relExpr sqlGenerator pe columns = SelectFrom $
    newSelect { attrs = SelectAttrs (ensureColumns (map (sqlBinding sqlGenerator) columns))
              , tables = oneTable (RelExpr (sqlExpr sqlGenerator pe))
              }

rebind :: SG.SqlGenerator -> Bool -> [(Symbol, HPQ.PrimExpr)] -> Select -> Select
rebind sqlGenerator star pes select = SelectFrom newSelect
  { attrs = selectAttrs (ensureColumns (map (sqlBinding sqlGenerator) pes))
  , tables = oneTable select
  }
  where selectAttrs = case star of
          True  -> SelectAttrsStar
          False -> SelectAttrs

forUpdate :: Select -> Select
forUpdate s = SelectFrom newSelect {
    tables = [(NonLateral, s)]
  , for = Just Update
  }
