# processstreamer library

A renewaled version of [cmd](https://github.com/xjj59307/cmd) library.
by Hiroshi Ukai

```xml
<dependency>
    <groupId>com.github.dakusui</groupId>
    <artifactId>processstreamer</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Build

To build this you need to install ```asciidoctor``` in advance

```shell script

sudo gem install asciidoctor -v 1.5.8
sudo gem install asciidoctor-diagram -v 1.5.18
sudo gem install concurrent-ruby

```
If you have already installed `asciidoctor` ruby library, `mvn javadoc:javadoc` will result in error complaining that `prepend` method is not defined. 
This is because `asciidoctor-diagram` tries to use the latest `asciidoctor`, which is 2.0.10 as of Aug/24/2019 and is not compatible with the latest `asciidoctor-diagram`, which is 1.5.18. 
