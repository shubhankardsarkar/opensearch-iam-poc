package com.bh.vc.opensearchiampoc.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bh.vc.opensearchiampoc.service.impl.OpenSearchServiceImpl;

@RestController
@RequestMapping("/api/v1")
public class OpenSearchController {

	@Autowired 
	OpenSearchServiceImpl openSearchService;
	
	@GetMapping("/test")
	public String test() {
		return "success";
	}
	
	@PostMapping(value = "/search", produces = "application/json; charset=utf-8")
	public ResponseEntity<String> searchPOST() throws IOException {
		String body = "";
		return openSearchService.search(body);
	}
}
