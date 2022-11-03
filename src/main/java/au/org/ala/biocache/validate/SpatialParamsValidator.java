package au.org.ala.biocache.validate;

import au.org.ala.biocache.dto.SpatialSearchRequestParams;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class SpatialParamsValidator implements ConstraintValidator<ValidSpatialParams, SpatialSearchRequestParams> {

    @Override
    public boolean isValid(SpatialSearchRequestParams spatialSearchRequestParams, ConstraintValidatorContext constraintValidatorContext) {

        // if lat, lon or radius are not null, then they all should be non-null
        if (spatialSearchRequestParams.getLat() != null
                || spatialSearchRequestParams.getLon() != null
                || spatialSearchRequestParams.getRadius() != null){

            if (spatialSearchRequestParams.getLat() == null
                    || spatialSearchRequestParams.getLon() == null
                    || spatialSearchRequestParams.getRadius() == null){
                return false;
            }
        }
        return true;
    }

    @Override
    public void initialize(ValidSpatialParams constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }
}
