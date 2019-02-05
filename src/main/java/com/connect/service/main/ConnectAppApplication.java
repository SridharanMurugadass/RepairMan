package com.connect.service.main;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.connect.service")
public class ConnectAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConnectAppApplication.class, args);
		System.out.println("heloooo");
	}
}
