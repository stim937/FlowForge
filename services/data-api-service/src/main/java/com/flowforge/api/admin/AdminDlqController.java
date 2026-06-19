package com.flowforge.api.admin;

import com.flowforge.api.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/admin/dlq-events")
public class AdminDlqController {

    private final AdminDlqService adminDlqService;

    public AdminDlqController(AdminDlqService adminDlqService) {
        this.adminDlqService = adminDlqService;
    }

    @GetMapping
    ApiResponse<List<DlqEventResponse>> listDlqEvents() {
        return ApiResponse.success(adminDlqService.listDlqEvents());
    }

    @PostMapping("/{eventId}/reprocess")
    ApiResponse<DlqEventResponse> reprocess(@PathVariable String eventId) {
        return ApiResponse.success(adminDlqService.reprocess(eventId));
    }
}
