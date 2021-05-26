package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.AssertionCode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class AssertionCodeSerializer extends JsonSerializer<AssertionCode> {

    @Override
    public void serialize(AssertionCode assertionCode, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {

        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", assertionCode.getName());

        if (assertionCode.isDeprecated()) {

            jsonGenerator.writeBooleanField("deprecated", assertionCode.isDeprecated());
            if (assertionCode.getNewName() != null) {
                jsonGenerator.writeStringField("newName", assertionCode.getNewName());
            }

        } else {

            jsonGenerator.writeNumberField("code", assertionCode.getCode());
            jsonGenerator.writeStringField("description", assertionCode.getDescription());
            jsonGenerator.writeStringField("category", assertionCode.getCategory());
            jsonGenerator.writeBooleanField("fatal", assertionCode.getFatal());
         }

        jsonGenerator.writeEndObject();
    }
}
