package com.example.KakfaSpring.KafKaSpringBootDemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan
public class KafKaSpringBootDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafKaSpringBootDemoApplication.class, args);
	}

}
