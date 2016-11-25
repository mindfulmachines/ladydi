# LadyDi

“It’s ironically all-singing, all-dancing, all-Walkman-wearing Diana who has got her first-class royal act together and proved herself not only the steadier parent, but the better half of our future monarchy.”

The goal of LadyDi is to help you Code Less, Build More. It is very easy to use and provides users with clean, automated Feature Generation and Selection for Apache Spark. It can also be used to identify and resolve issues that arise from causal and correlation interactions among features. It is a work in progress and still very much experimental so new versions may introduce breaking changes.

Please let us know what you think and follow our blog [Mindful Machines](http://www.mindfulmachines.io).

# Getting Started

If you're using maven then just add this or clone the repository (and then run mvn install):
```xml
<dependency>
    <groupId>io.mindfulmachines</groupId>
    <artifactId>ladydi_2.11</artifactId>
    <version>0.3-SNAPSHOT</version>
</dependency>
```

# Usage - Feature Generation
```scala
import ladydi.Features
```
From here, instead of setting specific names for input and output columns and creating tons of boilerplate in your code, you can just chain transformers together like so :

```scala
def category = {new StringNullEncoder() :: new StringIndexer() :: new OneHotEncoder() :: Nil}
def text = {new StringNullEncoder() :: new Tokenizer() ::
            new StopWordsRemover() ::
            new HashingTF().setNumFeatures(50) ::
            new StandardScaler()  ::  Nil}
def numeric = {new NumericNullEncoder() :: Nil}
```

# Detailed Description
Coming Soon

# The Future
 * Automated Causal and Correlated interactions need to be added 
 * Moving to SBT
 * Add more options to handling various null values
 * Move to more functional architecture
