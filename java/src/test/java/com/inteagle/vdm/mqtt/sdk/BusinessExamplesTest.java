package com.inteagle.vdm.mqtt.sdk;

import static org.junit.jupiter.api.Assertions.*;

import com.inteagle.examples.vdm.BusinessExamples;
import org.junit.jupiter.api.Test;

class BusinessExamplesTest {
  @Test
  void allFiveBusinessRecipesEncodeBothFormats() throws Exception {
    for (var format : PayloadFormat.values())
      for (var recipe : BusinessExamples.recipes(format).values())
        assertNotNull(new VdmCodec(format).encodeRpcRequest(recipe.method(), recipe.params(), 17));
  }
}
