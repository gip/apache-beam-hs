{-# LANGUAGE PolyKinds, TypeInType, MultiParamTypeClasses, AllowAmbiguousTypes,
FlexibleInstances, FunctionalDependencies, ScopedTypeVariables, GADTs, UndecidableInstances #-}
module Database.Apache.Beam.Executor.Pure where

import Control.Monad.Identity
import Data.Function (on)
import Data.List (sortBy, groupBy)
import Data.Ord (comparing)
import Database.Apache.Beam

data PurePipeline = PurePipeline

-- Pure pipelines never fail as everything is statically checked
type PurePipelineError = ()

instance Pipelinable PurePipelineError Identity True False PurePipeline where
  pipelineName _ = "Pure"
  execute c = return () -- This pure pipeline never fails
  executeWithResult = return . executePure

purePipeline :: Pipeline PurePipelineError Identity True False PurePipeline
purePipeline = runIdentity $ newPipeline

-- That never fails :-)
executePure :: PCollection b p a -> [a]
executePure (pcoll :> ptrans) = executeTransformPure ptrans $ executePure pcoll
executePure (_ :+ src) =
  case src of Empty -> []
              CreateBounded i l -> take i l
              CreateUnbounded l -> l
              SourceFile path f -> error "Typing error: source files are not valid in pure context"
executePure (pcoll :- sink) = executePure pcoll

executeTransformPure :: PTransform p b a b' a' -> [a] -> [a']
executeTransformPure ptrans value =
  case ptrans of
    Map f -> map f value
    FlatMap f -> join (map f value)
    ParDo f -> map f value
    _ :% ptrans -> executeTransformPure ptrans value
    CombineWith _ f ->
      case value of [] -> []
                    v : [] -> v : []
                    v : vs -> foldr f v vs : []
    CombineFn _ f e -> (e $ foldr f mempty value) : []
    CombinePerKey f -> map (\l -> let k = fst l in (k, f k $ snd l)) $ group value
    _ -> error "Not yet implemented"
  where
    group :: (Eq a, Ord a) => [(a, b)] -> [(a, [b])]
    group = map (\l -> (fst . head $ l, map snd l)) . groupBy ((==) `on` fst)
          . sortBy (comparing fst)
