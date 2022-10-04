package org.otmb.it;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class OtmbItApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(OtmbItApplication.class)
                .run(args);
    }
}
