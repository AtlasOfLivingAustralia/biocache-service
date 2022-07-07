package au.org.ala.biocache.config;

//import au.org.ala.ws.security.AlaWebServiceAuthFilter;
import au.ala.org.ws.security.AlaWsSecurityGrailsPluginConfiguration;
import com.nimbusds.jose.JWSAlgorithm;
import org.pac4j.core.authorization.authorizer.RequireAnyRoleAuthorizer;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.springframework.security.web.SecurityFilter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AnonymousAuthenticationProvider;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import javax.inject.Inject;
import javax.inject.Named;

//@AutoConfigureAfter({ AlaWsSecurityGrailsPluginConfiguration.class })
@Configuration
@ComponentScan(basePackageClasses = { AlaWsSecurityGrailsPluginConfiguration.class })
@EnableWebSecurity
@EnableGlobalMethodSecurity(securedEnabled = true)
@Order(1)
public class SecurityConfig extends WebSecurityConfigurerAdapter {

//    @Inject
//    AlaWebServiceAuthFilter alaWebServiceAuthFilter;
//
//    @Inject
//    @Named("pac4jJwtFilter")
//    FilterRegistrationBean pac4jJwtFilter;

    @Inject
    Config pac4jConfig;

    @Override
    protected void configure(HttpSecurity http) throws Exception {

//        Config pac4jConfig = new Config(clients);
//        pac4jConfig.setSessionStore(JEESessionStore.INSTANCE);
//        pac4jConfig.setWebContextFactory(JEEContextFactory.INSTANCE);
//
//        pac4jConfig.setAuthorizers(AnonymousAuthenticationProvider.class);

        SecurityFilter pac4jJwtFilter = new SecurityFilter(pac4jConfig, "JwtClient");

        http
                .antMatcher("/**")
//                .addFilterBefore(pac4jJwtFilter, AnonymousAuthenticationFilter.class)
                .addFilterAfter(pac4jJwtFilter, AnonymousAuthenticationFilter.class)
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.ALWAYS);

//        http.addFilterBefore(pac4jJwtFilter, BasicAuthenticationFilter.class);
//        http.authorizeRequests()
//                .antMatchers(
//                        "/",
//                        "/**"
//                ).permitAll()
//                .and().csrf().disable();

//        http.addFilterBefore(alaWebServiceAuthFilter, BasicAuthenticationFilter.class);
//        http.authorizeRequests()
//                .antMatchers(
//                        "/",
//                        "/**"
//                ).permitAll()
//                .and().csrf().disable();
    }
}
