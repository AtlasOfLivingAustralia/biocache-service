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
package org.ala.biocache.validate;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.ala.biocache.service.LoggerService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A custom validator to validate values associated with the LoggerService.
 * 
 * At the moment this is limited to values for the reason id and source id.
 * 
 * @author Natasha Carter <natasha.carter@csiro.au>
 */
public class LogTypeValidator implements ConstraintValidator<LogType,Integer>{
    @Autowired
    private LoggerService loggerService;
    
    private String type;
    @Override
    public void initialize(LogType logType) {
        this.type = logType.type();
    }

    @Override
    public boolean isValid(Integer value, ConstraintValidatorContext context) {
        
        if(value == null){
          //Values can be null so is valid
            return true;
        }
        //check to see if the corresponding "type" contains the value
        if("reason".equals(type)){
            return loggerService.getReasonIds().contains(value);
        } else if("source".equals(type)){
            return loggerService.getSourceIds().contains(value);
        }
        //otherwise it is always valid
        return true;
    }

}
