package com.dlink.health.dsg;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.JsonParserDelegate;

import java.io.IOException;
import java.util.List;

public class KeysToLowercaseParser extends JsonParserDelegate {

    private List<String> schemaFields;

    public KeysToLowercaseParser(JsonParser d) {
        super(d);
    }

    KeysToLowercaseParser(JsonParser d, List<String> schemaFields) {
        super(d);
        this.schemaFields = schemaFields;
    }

    @Override
    public String getCurrentName() throws IOException {
        return this.hasTokenId(5) && this.schemaFields.contains(this.delegate.getCurrentName().toLowerCase()) ? this.delegate.getCurrentName().toLowerCase() : this.delegate.getCurrentName();
    }

    @Override
    public String getText() throws IOException {
        return this.hasTokenId(5) && this.schemaFields.contains(this.delegate.getCurrentName().toLowerCase()) ? this.delegate.getText().toLowerCase() : this.delegate.getText();
    }
}
