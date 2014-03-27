/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.validate;

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
