package au.org.ala.biocache.controller;

import au.org.ala.biocache.dto.AssertionCodes;
import au.org.ala.biocache.dto.QualityAssertion;
import au.org.ala.biocache.dto.UserAssertions;
import au.org.ala.biocache.service.AssertionService;
import au.org.ala.biocache.util.SolrUtils;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import au.org.ala.biocache.web.AssertionController;
import au.org.ala.ws.security.AlaUser;
import au.org.ala.ws.security.AlaWebServiceAuthFilter;
import au.org.ala.ws.security.JwtProperties;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration tests for occurrence services.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
public class AssertionsControllerIT extends TestCase {

    static {
        System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
    }

    public int USER_ASSERTION_CODES_LENGTH;
    public int ALL_CODES_LENGTH;
    private int DEPRECATED_CODES_LENGTH;

    @Autowired
    AssertionController assertionController;

    @Autowired
    AlaWebServiceAuthFilter alaWebServiceAuthFilter;

    @Autowired
    FieldMappingUtil fieldMappingUtil;

    AssertionService assertionService;

    @Autowired
    WebApplicationContext wac;

    MockMvc mockMvc;

    final static AlaUser TEST_USER =
            new AlaUser("test@test.com","Tester",null,null, null, null);

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        SolrUtils.setupIndex();
    }

    @Before
    public void setup() {
        USER_ASSERTION_CODES_LENGTH = AssertionCodes.userAssertionCodes.length;
        ALL_CODES_LENGTH = AssertionCodes.allAssertionCodes.size();
        DEPRECATED_CODES_LENGTH = fieldMappingUtil.getFieldValueMappingStream("assertions").collect(Collectors.toList()).size();

        assertionService = mock(AssertionService.class);
        ReflectionTestUtils.setField(assertionController, "assertionService", assertionService);

        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
    }

    @Test
    public void getAssertionCodes() throws Exception {
        this.mockMvc.perform(get("/assertions/codes")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(ALL_CODES_LENGTH));
    }

    @Test
    public void getAssertionCodesWithDeprecated() throws Exception {
        this.mockMvc.perform(get("/assertions/codes")
                .param("deprecated", "true")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(ALL_CODES_LENGTH + DEPRECATED_CODES_LENGTH));
    }

    @Test
    public void getUserCodes() throws Exception {
        this.mockMvc.perform(get("/assertions/user/codes")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(USER_ASSERTION_CODES_LENGTH));

        this.mockMvc.perform(get("/assertions/user/codes/")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(USER_ASSERTION_CODES_LENGTH));
    }

    @Test
    public void testAddSingle() throws Exception {

//        JwtProperties jwtProperties = (JwtProperties) ReflectionTestUtils.getField(alaWebServiceAuthFilter, "jwtProperties");
//        jwtProperties.setFallbackToLegacyBehaviour(false);

        // add succeed
        when(assertionService.addAssertion(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        this.mockMvc.perform(post("/occurrences/assertions/add")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("code", "code")
                .param("userId", "95187")
                .param("userDisplayName", "xuanyu huang"))
                .andExpect(status().isBadRequest());

        // add fail (record uuid wrong)
        when(assertionService.addAssertion(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(new QualityAssertion()));
        this.mockMvc.perform(post("/occurrences/assertions/add")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("code", "code")
                .param("userId", "95187")
                .param("userDisplayName", "xuanyu huang"))
                .andExpect(status().isCreated());

        // exception
        when(assertionService.addAssertion(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(IOException.class);
        this.mockMvc.perform(post("/occurrences/assertions/add")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("code", "code")
                .param("userId", "95187")
                .param("userDisplayName", "xuanyu huang"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testDeleteSingle() throws Exception {

//        JwtProperties jwtProperties = (JwtProperties) ReflectionTestUtils.getField(alaWebServiceAuthFilter, "jwtProperties");
//        jwtProperties.setFallbackToLegacyBehaviour(true);

        // delete succeed
//        when(apiKeyService.isValidKey(Mockito.any())).thenReturn(new AuthenticatedUser(
//                "test@test.com","Tester",null,null));
        when(assertionService.deleteAssertion(Mockito.any(), Mockito.any())).thenReturn(true);
        this.mockMvc.perform(post("/occurrences/assertions/delete")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("assertionUuid", "assertionUuid"))
                .andExpect(status().isOk());

        // record not found
//        when(apiKeyService.isValidKey(Mockito.any())).thenReturn(new AuthenticatedUser(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any()));
        when(assertionService.deleteAssertion(Mockito.any(), Mockito.any())).thenReturn(false);
        this.mockMvc.perform(post("/occurrences/assertions/delete")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("assertionUuid", "assertionUuid"))
                .andExpect(status().isBadRequest());

        // exception
//        when(apiKeyService.isValidKey(Mockito.any())).thenReturn(new AuthenticatedUser(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any()));
        when(assertionService.deleteAssertion(Mockito.any(), Mockito.any())).thenThrow(IOException.class);
        this.mockMvc.perform(post("/occurrences/assertions/delete")
                .principal(TEST_USER)
                .param("recordUuid", "recordUuid")
                .param("apiKey", "apiKey")
                .param("assertionUuid", "assertionUuid"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testGetSingle() throws Exception {
        // to stub val to be returned
        QualityAssertion qa = new QualityAssertion();

        when(assertionService.getAssertion("recordUuid", "assertionUuid")).thenReturn(qa);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions/assertionUuid"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.uuid").value(qa.getUuid()));

        // get null
        when(assertionService.getAssertion("recordUuid", "assertionUuid")).thenReturn(null);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions/assertionUuid"))
                .andExpect(status().isNotFound());

        // exception
        when(assertionService.getAssertion("recordUuid", "assertionUuid")).thenThrow(IOException.class);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions/assertionUuid"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testGetAssertions() throws Exception {
        UserAssertions qas = new UserAssertions();
        qas.add(new QualityAssertion());
        qas.add(new QualityAssertion());

        // get assertions (size 2)
        when(assertionService.getAssertions("recordUuid")).thenReturn(qas);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions"))
                 .andExpect(jsonPath("$.length()").value(2))
                 .andExpect(jsonPath("$[0]['uuid']").value(qas.get(0).getUuid()))
                 .andExpect(jsonPath("$[1]['uuid']").value(qas.get(1).getUuid()));

        qas.clear();
        // get 0 assertions
        when(assertionService.getAssertions("recordUuid")).thenReturn(qas);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions"))
                .andExpect(jsonPath("$.length()").value(0));

        // exception
        when(assertionService.getAssertions("recordUuid")).thenThrow(IOException.class);
        this.mockMvc.perform(get("/occurrences/recordUuid/assertions"))
                .andExpect(status().isInternalServerError());

    }

    @Test
    public void testBulkAdd() throws Exception {

//        JwtProperties jwtProperties = (JwtProperties) ReflectionTestUtils.getField(alaWebServiceAuthFilter, "jwtProperties");
//        jwtProperties.setFallbackToLegacyBehaviour(false);

        when(assertionService.bulkAddAssertions(Mockito.any(), Mockito.any())).thenReturn(true);
        this.mockMvc.perform(post("/bulk/assertions/add")
                .principal(TEST_USER)
                .param("apiKey", "apiKey")
                .param("assertions", "[\n" +
                        "{\n" +
                        "\t\"code\": 0,\n" +
                        "\t\"comment\": \"comemnt 1 -alex - c0ee1a86-1df6-40b2-950c-bdde40b1c46e\",\n" +
                        "\t\"recordUuid\": \"c0ee1a86-1df6-40b2-950c-bdde40b1c46e\"\n" +
                        "},\n" +
                        "{\n" +
                        "\t\"code\": 2000,\n" +
                        "\t\"comment\": \"comemnt 2 -alex - c0ee1a86-1df6-40b2-950c-bdde40b1c46e\",\n" +
                        "\t\"recordUuid\": \"c0ee1a86-1df6-40b2-950c-bdde40b1c46e\"\n" +
                        "},\n" +
                        "{\n" +
                        "\t\"code\": 0,\n" +
                        "\t\"comment\": \"comemnt 1 -alex\",\n" +
                        "\t\"recordUuid\": \"909ef431-06f1-4d0c-bd41-8be1306e2e47\"\n" +
                        "}\n" +
                        "]")
                .param("userId", "95187")
                .param("userDisplayName", "xuanyu huang"));


        ArgumentCaptor<String> myString = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<UserAssertions> myUserAssertions = ArgumentCaptor.forClass(UserAssertions.class);
        Mockito.verify(assertionService, times(2)).bulkAddAssertions(myString.capture(), myUserAssertions.capture());

        List<String> allUuids = myString.getAllValues();
        allUuids.sort(String::compareTo);
        assert(allUuids.get(0).equals("909ef431-06f1-4d0c-bd41-8be1306e2e47"));
        assert(allUuids.get(1).equals("c0ee1a86-1df6-40b2-950c-bdde40b1c46e"));
    }
}
