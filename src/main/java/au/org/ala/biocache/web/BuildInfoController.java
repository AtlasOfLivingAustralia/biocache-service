package au.org.ala.biocache.web;

import io.swagger.annotations.Api;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Controller
@Api(value = "BuildInfo",  description = "Build information", hidden = true, tags = { "Build" })
class BuildInfoController {

    private final static Logger logger = Logger.getLogger(BuildInfoController.class);

    @RequestMapping("/buildInfo")
    public void buildInfo(Model model) {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try (
                InputStream buildInfoStream = classLoader.getResourceAsStream("/buildInfo.properties");
                InputStream runtimeEnvironmentStream = classLoader.getResourceAsStream("/runtimeEnvironment.properties");
            ) {

            if (runtimeEnvironmentStream != null) {

                try {
                    Properties runtimeEnvironmentProperties = new Properties();
                    runtimeEnvironmentProperties.load(runtimeEnvironmentStream);
                    runtimeEnvironmentProperties.setProperty("java.version", System.getProperty("java.version"));
                    model.addAttribute("runtimeEnvironment", runtimeEnvironmentProperties);
                } catch (IOException e) {
                    logger.error("failed to read 'runtimeEnvironment.properties' resource", e);
                }
            } else {
                Properties runtimeEnvironmentProperties = new Properties();
                runtimeEnvironmentProperties.setProperty("BuildInformation", "UNAVAILABLE");
                runtimeEnvironmentProperties.setProperty("java.version", System.getProperty("java.version"));
                model.addAttribute("runtimeEnvironment", runtimeEnvironmentProperties);
            }

            if (buildInfoStream != null) {

                try {
                    Properties buildInfoProperties = new Properties();
                    buildInfoProperties.load(buildInfoStream);
                    model.addAttribute("buildInfo", buildInfoProperties);
                } catch (IOException e) {
                    logger.error("failed to read 'buildInfo.properties' resource", e);
                }
            }

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
//        return new ModelAndView("buildInfo", model);
    }
}