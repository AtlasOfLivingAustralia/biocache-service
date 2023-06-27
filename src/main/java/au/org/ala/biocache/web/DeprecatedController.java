package au.org.ala.biocache.web;

import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.SpatialSearchRequestDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import au.org.ala.biocache.dto.TaxaCountDTO;
import au.org.ala.biocache.util.converter.FqField;
import io.swagger.v3.oas.annotations.Operation;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Controller
public class DeprecatedController {

    @Inject
    protected OccurrenceController occurrenceController;
    @Inject
    protected WMSController wmsController;

    /**
     * Webservice to report the occurrence counts for the supplied list of taxa
     */
    @Operation(tags="Deprecated")
    @RequestMapping(value = {  "/occurrences/taxaCount.json" }, method = {RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody
    Map<String, Integer> occurrenceSpeciesCounts(
            @RequestParam(value = "listOfGuids", required = true, defaultValue = "") String listOfGuids,
            @FqField @RequestParam(value = "fq", required = false) String[] filterQueries,
            @RequestParam(defaultValue = "\n") String separator,
            HttpServletResponse response,
            HttpServletRequest request
    ) throws Exception {
        return occurrenceController.occurrenceSpeciesCounts(
                listOfGuids, filterQueries, separator, response);
    }

    @Operation( tags = "Deprecated")
    @Deprecated
    @GetMapping(value = {"/occurrences/download.json*", "/occurrences/index/download*","/occurrences/index/download.json*"})
    public void occurrenceDownload( @ParameterObject DownloadRequestParams requestParams,
                                    @RequestParam(required = false, defaultValue = "true") Boolean zip,
                                              BindingResult result,
                                              Model model,
                                              HttpServletResponse response,
                                              HttpServletRequest request) throws Exception {
        occurrenceController.occurrenceDownload(requestParams, zip, result, model, response, request);
    }

    /**
     * Returns a comparison of the occurrence versions.
     *
     * @param uuid
     * @return
     */
    @Operation(summary = "Deprecated path - Comparison of verbatim and interpreted record fields", tags = "Deprecated")
    @RequestMapping(value = { "/occurrence/compare.json*" }, method = RequestMethod.GET)
    public @ResponseBody
    Object compareOccurrenceVersions(@RequestParam(value = "uuid", required = true) String uuid,
                                     HttpServletResponse response) throws Exception {
        return occurrenceController.showOccurrence(uuid, response);
    }

    /**
     * Get query bounding box as JSON array containing:
     * min longitude, min latitude, max longitude, max latitude
     *
     * @param requestParams
     * @param response
     * @return
     * @throws Exception
     */
    @Operation(summary = "Deprecated path - Get query bounding box as JSON", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/bounds",
            "/mapping/bounds.json",
            "/webportal/bounds",
            "/mapping/bounds.json" }, method = RequestMethod.GET)
    public
    @ResponseBody
    double[] jsonBoundingBox(
            SpatialSearchRequestParams requestParams,
            HttpServletResponse response)
            throws Exception {
        return wmsController.jsonBoundingBox(requestParams, response);
    }

    /**
     * JSON web service that returns a list of species and record counts for a given location search
     *
     * @throws Exception
     */
    @Operation(summary = "Deprecated path - JSON web service that returns a list of species and record counts for a given location search", tags = "Deprecated")
    @RequestMapping(value = {
            "/webportal/species",
            "/webportal/species.json",
            "/mapping/species.json"}, method = RequestMethod.GET)
    public void listSpecies(@ParameterObject SpatialSearchRequestParams params,
                          HttpServletResponse response) throws Exception {
        wmsController.listSpecies(params, response);
    }
}
