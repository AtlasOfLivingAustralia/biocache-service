package au.org.ala.biocache.config;

import au.org.ala.ws.security.AlaWebServiceAuthFilter;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import javax.inject.Inject;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(securedEnabled = true)
@Order(1)
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    @Inject
    AlaWebServiceAuthFilter alaWebServiceAuthFilter;

    @Override
    protected void configure(HttpSecurity http) throws Exception {

        http.addFilterBefore(alaWebServiceAuthFilter, BasicAuthenticationFilter.class);
        http.authorizeRequests()
                .antMatchers(
                        "/",
                        "/**"
                ).permitAll()
                .and().csrf().disable();
    }
}
