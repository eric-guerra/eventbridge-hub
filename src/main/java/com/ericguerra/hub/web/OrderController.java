package com.ericguerra.hub.web;

import com.ericguerra.hub.orders.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    public record PlaceReq(String customerId, long totalCents, List<OrderService.Line> lines) {}
    public record CancelReq(String reason) {}

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> place(@RequestBody PlaceReq r) {
        return Map.of("id", orders.place(r.customerId(), r.totalCents(), r.lines()));
    }

    @PostMapping("/{id}/cancel")
    public void cancel(@PathVariable UUID id, @RequestBody CancelReq r) {
        orders.cancel(id, r.reason());
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable UUID id) {
        return orders.get(id);
    }
}
