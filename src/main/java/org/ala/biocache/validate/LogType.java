package org.ala.biocache.validate;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;
/**
 * 
 * The custom validation annotation to be used for Log Types.
 * 
 * All undocumented methods are boilerplate code required by JSR 303 
 * 
 * @author Natasha Carter <natasha.carter@csiro.au>
 *
 */
@Documented
@Constraint(validatedBy = LogTypeValidator.class)
@Target( { ElementType.METHOD, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface LogType {

    String message() default "{LogType}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
    /**
     * Supported values are "reason" and "source" at the moment.
     * @return
     */
    String type() default "reason";
    
}
