# apache-beam-hs

This package is an (experimental) implementation of [Apache Beam](https://beam.apache.org/) in Haskell.

The connection to the back-end is currently work-in-progress - and until this is done this code remains pretty much useless! As an example, it is possible to run the equivalent of the [MinimalWordCount example](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example) using a local and pure pipeline. The transformations for that pipeline can be found in this [file](https://github.com/gip/apache-beam-hs/blob/master/examples/haskell/src/MinimalWordCountPure.hs) and look like this:

```haskell
minimalWordCount p input =
  p :+ CreateUnbounded input
    :> FlatMap (\x -> join ((x :: String) =~ "[A-Za-z\']+" :: [[String]]))
    :> Map (\x -> (x, 1))
    :> CombinePerKey (\k l -> length l)
    :> Map (\(k,v) -> concat [k, ": ", show v])
```
