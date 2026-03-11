package com.aiwf.base.web;

import com.aiwf.base.service.BackendCapabilitiesService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/backend")
public class BackendController {

    private final BackendCapabilitiesService capabilities;

    public BackendController(BackendCapabilitiesService capabilities) {
        this.capabilities = capabilities;
    }

    @GetMapping("/capabilities")
    public Map<String, Object> capabilities() {
        return capabilities.snapshot();
    }
}
