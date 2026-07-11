package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import org.springframework.stereotype.Service;

@Service
public class ValidationService {

    public boolean isValid(OrderRequest request) {
        return request.customer() != null && !request.customer().isBlank()
                && request.country() != null && !request.country().isBlank()
                && request.items() > 0 && request.amount() > 0;
    }

    public OrderRequest normalize(OrderRequest request) {
        return request.withCustomer(request.customer().trim())
                .withCountry(request.country().trim().toUpperCase());
    }
}
