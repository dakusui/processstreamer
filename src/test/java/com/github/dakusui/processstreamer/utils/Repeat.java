package com.github.dakusui.processstreamer.utils;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Target({
    java.lang.annotation.ElementType.METHOD
})
public @interface Repeat {
  int times();
}