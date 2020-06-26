package au.org.ala.biocache.web;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Controller
class BuildInfoController {

    private final static Logger logger = Logger.getLogger(BuildInfoController.class);

    @RequestMapping(value="/buildInfo", method = RequestMethod.GET)
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
    }
}