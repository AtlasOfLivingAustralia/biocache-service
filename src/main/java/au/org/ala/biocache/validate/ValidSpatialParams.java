package au.org.ala.biocache.validate;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Target(TYPE)
@Retention(RUNTIME)
@Constraint(validatedBy = { SpatialParamsValidator.class })
public @interface ValidSpatialParams {

    String message() default "{com.example.validation.ValidAddress.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
