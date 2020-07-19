{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}

module Opaleye.Internal.MaybeFields where

import qualified Opaleye.Internal.Column as IC
import qualified Opaleye.Constant as Constant
import qualified Opaleye.Internal.Distinct as D
import qualified Opaleye.Internal.RunQuery as RQ
import qualified Opaleye.Internal.Unpackspec as U
import qualified Opaleye.Internal.Values as V
import qualified Opaleye.Column
import           Opaleye.Operators ((.&&))
import           Opaleye.Internal.Operators (IfPP, ifExplict)
import qualified Opaleye.SqlTypes
import           Opaleye.SqlTypes (SqlBool, IsSqlType)

import           Control.Monad (replicateM_)

import qualified Data.Profunctor as P
import qualified Data.Profunctor.Product as PP
import qualified Data.Profunctor.Product.Default as PP

import qualified Database.PostgreSQL.Simple.FromRow as PGSR

-- | The Opaleye analogue of 'Data.Maybe.Maybe'
data MaybeFields fields =
  MaybeFields {
    mfPresent :: Opaleye.Column.Column Opaleye.SqlTypes.SqlBool
  , mfFields  :: fields
  }
  deriving Functor

instance Applicative MaybeFields where
  pure fields = MaybeFields { mfPresent = Opaleye.SqlTypes.sqlBool True
                            , mfFields  = fields
                            }
  MaybeFields t f <*> MaybeFields t' a =
    MaybeFields {
      mfPresent = t .&& t'
    , mfFields  = f a
    }

instance Monad MaybeFields where
  return = pure
  MaybeFields t a >>= f = case f a of
    MaybeFields t' b -> MaybeFields (t .&& t') b

-- | The Opaleye analogue of 'Data.Maybe.Nothing'.
nothingFields :: PP.Default V.Nullspec a a => MaybeFields a
nothingFields = nothingFieldsExplicit def
  where def :: PP.Default V.Nullspec a a => V.Nullspec a a
        def = PP.def

-- | The Opaleye analogue of 'Data.Maybe.Just'.  Equivalent to
-- 'Control.Applicative.pure'.
justFields :: a -> MaybeFields a
justFields = pure

-- | The Opaleye analogue of 'Data.Maybe.maybe'
maybeFields :: PP.Default IfPP b b => b -> (a -> b) -> MaybeFields a -> b
maybeFields = maybeFieldsExplicit PP.def

-- | The Opaleye analogue of 'Data.Maybe.fromMaybe'
fromMaybeFields :: PP.Default IfPP b b => b -> MaybeFields b -> b
fromMaybeFields = fromMaybeFieldsExplicit PP.def

maybeFieldsExplicit :: IfPP b b' -> b -> (a -> b) -> MaybeFields a -> b'
maybeFieldsExplicit ifpp b f mf =
  ifExplict ifpp (mfPresent mf) (f (mfFields mf)) b

fromMaybeFieldsExplicit :: IfPP b b -> b -> MaybeFields b -> b
fromMaybeFieldsExplicit ifpp = flip (maybeFieldsExplicit ifpp) id

nothingFieldsExplicit :: V.Nullspec a b -> MaybeFields b
nothingFieldsExplicit n = MaybeFields {
    mfPresent = Opaleye.SqlTypes.sqlBool False
  , mfFields  = V.nullFields n
  }

fromFieldsMaybeFields :: RQ.FromFields fields haskells
                      -> RQ.FromFields (MaybeFields fields) (Maybe haskells)
fromFieldsMaybeFields (RQ.QueryRunner u p c) = RQ.QueryRunner u' p' c'
  where u' = () <$ productProfunctorMaybeFields U.unpackspecColumn u

        p' = \mf -> do
          hIsPresent <- PGSR.field

          case hIsPresent of
            True  -> Just <$> p (mfFields mf)
            False -> Nothing <$ replicateM_ (c (mfFields mf))
                                            (PGSR.fieldWith (\_ _ -> pure ()))

        c' = \mf -> c (mfFields mf) + 1

-- | This is not safe in general because it relies on p not doing
-- anything observable with the @a@s if @mfPresent@ is false.  In
-- particular, it won't work for 'D.Distinctspec' because it does
-- indeed look at the @mfFields@ to check distinctness.
productProfunctorMaybeFields :: PP.ProductProfunctor p
                             => p (Field SqlBool) (Field SqlBool)
                             -> p a b
                             -> p (MaybeFields a) (MaybeFields b)
productProfunctorMaybeFields b p = MaybeFields PP.***$ P.lmap mfPresent b
                                               PP.**** P.lmap mfFields p

nullspecMaybeFields :: V.Nullspec a b
                    -> V.Nullspec (MaybeFields a) (MaybeFields b)
nullspecMaybeFields = productProfunctorMaybeFields V.nullspecField

unpackspecMaybeFields :: U.Unpackspec a b
                      -> U.Unpackspec (MaybeFields a) (MaybeFields b)
unpackspecMaybeFields = productProfunctorMaybeFields U.unpackspecField

valuesspecMaybeFields :: V.ValuesspecSafe a b
                      -> V.ValuesspecSafe (MaybeFields a) (MaybeFields b)
valuesspecMaybeFields = productProfunctorMaybeFields V.valuesspecField

toFieldsMaybeFields :: V.Nullspec a b
                    -> Constant.ToFields a b
                    -> Constant.ToFields (Maybe a) (MaybeFields b)
toFieldsMaybeFields n p = Constant.Constant $ \case
  Nothing -> nothingFieldsExplicit n
  Just a  -> justFields (Constant.constantExplicit p a)

-- | This is only safe if d is OK with having nulls passed through it
-- when they claim to be non-null.
unWithNulls :: PP.ProductProfunctor p
            => p (Field SqlBool) (Field SqlBool)
            -> WithNulls p a b
            -> p (MaybeFields a) (MaybeFields b)
unWithNulls b (WithNulls d) =
    MaybeFields PP.***$ P.lmap mfPresent b
                PP.**** d

newtype WithNulls p a b =
  WithNulls (p (MaybeFields a) b)

-- | This is only safe if d is OK with having nulls passed through it
-- when they claim to be non-null.
mapMaybeFieldsWithNulls :: PP.ProductProfunctor p
                        => p (Field SqlBool) (Field SqlBool)
                        -> WithNulls p a b
                        -> WithNulls p (MaybeFields a) (MaybeFields b)
mapMaybeFieldsWithNulls b d =
  MaybeFields <$> P.lmap mfPresent (withNullsField b)
              <*> P.lmap mfFields d

-- | This is only safe if d is OK with having nulls passed through it
-- when they claim to be non-null.
withNullsField :: (IsSqlType a, P.Profunctor p)
               => p (IC.Column a) (IC.Column a)
               -> WithNulls p (IC.Column a) (IC.Column a)
withNullsField col = result
  where result = WithNulls (P.lmap (\(MaybeFields b c) ->
                                      ifExplict PP.def b c nullC) col)
        nullC = IC.Column (V.nullPE (columnProxy result))

        columnProxy :: f (IC.Column sqlType) -> Maybe sqlType
        columnProxy _ = Nothing

instance P.Profunctor p => P.Profunctor (WithNulls p) where
  dimap f g (WithNulls d) = WithNulls (P.dimap (fmap f) g d)

instance P.Profunctor p => Functor (WithNulls p a) where
  fmap = P.rmap

instance PP.ProductProfunctor p => Applicative (WithNulls p a) where
  pure = WithNulls . PP.purePP
  WithNulls fd <*> WithNulls xd = WithNulls (fd PP.**** xd)

instance PP.ProductProfunctor p => PP.ProductProfunctor (WithNulls p) where
  purePP = pure
  (****) = (<*>)

instance PP.SumProfunctor p => PP.SumProfunctor (WithNulls p) where
  WithNulls ff +++! WithNulls xf =
    WithNulls (flip P.lmap (ff PP.+++! xf) $ \case
                  MaybeFields b (Left l)  -> Left  (MaybeFields b l)
                  MaybeFields b (Right r) -> Right (MaybeFields b r))

instance PP.Default RQ.QueryRunner fields haskells
  => PP.Default RQ.QueryRunner (MaybeFields fields) (Maybe haskells) where
  def = fromFieldsMaybeFields PP.def

instance PP.Default U.Unpackspec a b
  => PP.Default U.Unpackspec (MaybeFields a) (MaybeFields b) where
  def = unpackspecMaybeFields PP.def

instance PP.Default V.ValuesspecSafe a b
  => PP.Default V.ValuesspecSafe (MaybeFields a) (MaybeFields b) where
  def = valuesspecMaybeFields PP.def

instance (PP.Default Constant.Constant a b, PP.Default V.Nullspec a b)
  => PP.Default Constant.Constant (Maybe a) (MaybeFields b) where
  def = toFieldsMaybeFields PP.def PP.def

instance (P.Profunctor p, IsSqlType a, PP.Default p (IC.Column a) (IC.Column a))
  => PP.Default (WithNulls p) (IC.Column a) (IC.Column a) where
  def = withNullsField PP.def

instance PP.Default (WithNulls D.Distinctspec) a b
  => PP.Default D.Distinctspec (MaybeFields a) (MaybeFields b) where
  def = unWithNulls PP.def PP.def
