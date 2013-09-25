package org.ala.biocache.validate;

import java.util.Arrays;

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
 *
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
