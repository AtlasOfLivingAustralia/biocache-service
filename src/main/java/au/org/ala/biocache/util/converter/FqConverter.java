package au.org.ala.biocache.util.converter;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;

import java.util.Set;

/**
 * Only for use by the `fq` URL parameter. Only applies to fields annotated with `FqField` and requiring
 * `String` to `String[]` conversion.
 *
 * Overrides the default behaviour of WebConversionService where URL parameters are split by `,` when there is
 * only a single term and the target is an array.
 */
public class FqConverter implements ConditionalGenericConverter {

        public Set<ConvertiblePair> getConvertibleTypes() {
            // Returning null to add this converter to the global pool of converters, making it apply before non-global.
            return null;
        }

        public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
            return (targetType.hasAnnotation(FqField.class) &&
                            "java.lang.String".equals(sourceType.getName()) &&
                            "java.lang.String[]".equals(targetType.getName()));
        }

        public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
            return source == null ? null : new String[] { source.toString() };
        }
    }
