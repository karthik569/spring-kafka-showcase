package com.example.springkafka.service;

import com.example.springkafka.dto.OrderEvent;
import com.example.springkafka.dto.SagaInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrator implementing the Distributed Saga Pattern.
 * <p>
 * Coordinates multi-step asynchronous transactions across simulated microservices
 * (Order Service -> Payment Service -> Inventory Service).
 * Executes compensating transactions (automated rollback) when downstream steps fail.
 */
@Service
public class SagaOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestratorService.class);

    private final KafkaProducerService producerService;
    private final Map<String, SagaInstance> activeSagas = new ConcurrentHashMap<>();

    public SagaOrchestratorService(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * Initiates a new Saga transaction for order checkout.
     *
     * @param order the order to be processed
     * @param forcePaymentFail synthetic failure flag to test compensating rollback
     * @return the resulting SagaInstance
     */
    public SagaInstance startOrderSaga(OrderEvent order, boolean forcePaymentFail) {
        String sagaId = "SAGA-" + UUID.randomUUID().toString().substring(0, 8);
        double total = order.price() * order.quantity();

        SagaInstance saga = SagaInstance.start(sagaId, order.orderId(), order.customerId(), total);
        activeSagas.put(sagaId, saga);

        log.info("[SAGA-ORCHESTRATOR] Started sagaId={} for orderId={} amount={}", sagaId, order.orderId(), total);

        // Step 1: Dispatched Order Placed command
        producerService.sendAsyncRaw("saga.orders.commands", sagaId, "ACTION:PLACE_ORDER:" + order.orderId());
        saga = saga.addStep("STEP 1: Order initialized", "PAYMENT_PENDING");

        // Step 2: Payment Execution / Compensation Evaluation
        if (forcePaymentFail) {
            log.warn("[SAGA-ORCHESTRATOR] Payment simulated failure for sagaId={}! Initiating compensating transaction...", sagaId);
            saga = saga.addStep("STEP 2: Payment processing failed ($" + total + ")", "COMPENSATING");

            // Compensating transaction: Cancel order and release reservations
            producerService.sendAsyncRaw("saga.orders.commands", sagaId, "ACTION:COMPENSATE_ORDER_CANCEL:" + order.orderId());
            saga = saga.addStep("COMPENSATION: Order cancelled and customer notified", "COMPENSATED");
            activeSagas.put(sagaId, saga);
            return saga;
        }

        // Normal successful checkout path
        saga = saga.addStep("STEP 2: Payment authorized ($" + total + ")", "INVENTORY_PENDING");
        producerService.sendAsyncRaw("saga.inventory.commands", sagaId, "ACTION:RESERVE_STOCK:" + order.skuCode());

        saga = saga.addStep("STEP 3: Warehouse stock reserved for SKU " + order.skuCode(), "COMPLETED");
        producerService.sendAsyncRaw("saga.events.completed", sagaId, "SAGA_COMPLETED:" + order.orderId());

        activeSagas.put(sagaId, saga);
        log.info("[SAGA-ORCHESTRATOR] SagaId={} completed successfully!", sagaId);
        return saga;
    }

    public Optional<SagaInstance> getSaga(String sagaId) {
        return Optional.ofNullable(activeSagas.get(sagaId));
    }

    public List<SagaInstance> getAllSagas() {
        return new ArrayList<>(activeSagas.values());
    }
}
