package au.org.ala.biocache.util;

import au.org.ala.biocache.dto.IndexFieldDTO;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class IndexFieldSerializer extends JsonSerializer<IndexFieldDTO> {


    @Override
    public void serialize(IndexFieldDTO indexFieldDTO, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {

        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", indexFieldDTO.getName());

        if (indexFieldDTO.isDeprecated()) {
            jsonGenerator.writeBooleanField("deprecated", indexFieldDTO.isDeprecated());
        }

        if (indexFieldDTO.isDeprecated() && indexFieldDTO.getNewFieldName() != null) {
            jsonGenerator.writeStringField("newFieldName", indexFieldDTO.getNewFieldName());
        }

        if (!indexFieldDTO.isDeprecated()) {

            if (indexFieldDTO.getJsonName() != null) { jsonGenerator.writeStringField("jsonName", indexFieldDTO.getJsonName()); }
            if (indexFieldDTO.getDwcTerm() != null) { jsonGenerator.writeStringField("dwcTerm", indexFieldDTO.getDwcTerm()); }
            jsonGenerator.writeStringField("downloadName", indexFieldDTO.getDownloadName());
            jsonGenerator.writeStringField("dataType", indexFieldDTO.getDataType());
            if (indexFieldDTO.getClasss() != null) { jsonGenerator.writeStringField("classs", indexFieldDTO.getClasss()); }
            jsonGenerator.writeBooleanField("indexed", indexFieldDTO.isIndexed());
            jsonGenerator.writeBooleanField("docvalue", indexFieldDTO.isDocvalue());
            jsonGenerator.writeBooleanField("stored", indexFieldDTO.isStored());
            jsonGenerator.writeBooleanField("multivalue", indexFieldDTO.isMultivalue());
            if (indexFieldDTO.isI18nValues() != null) { jsonGenerator.writeBooleanField("i18nValues", indexFieldDTO.isI18nValues()); }
            if (indexFieldDTO.getDescription() != null) { jsonGenerator.writeStringField("description", indexFieldDTO.getDescription()); }
            if (indexFieldDTO.getDownloadDescription() != null) { jsonGenerator.writeStringField("downloadDescription", indexFieldDTO.getDownloadDescription()); }
            if (indexFieldDTO.getInfo() != null) { jsonGenerator.writeStringField("info", indexFieldDTO.getInfo()); }
            if (indexFieldDTO.getInfoUrl() != null) { jsonGenerator.writeStringField("infoUrl", indexFieldDTO.getInfoUrl()); }
            if (indexFieldDTO.getNumberDistinctValues() != null) { jsonGenerator.writeNumberField("numberDistinctValues", indexFieldDTO.getNumberDistinctValues()); }
        }

        jsonGenerator.writeEndObject();
    }
}
