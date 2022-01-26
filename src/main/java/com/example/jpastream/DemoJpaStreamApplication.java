package com.example.jpastream;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hibernate.jpa.QueryHints.HINT_FETCH_SIZE;

interface UserRepository extends JpaRepository<User, Long> {

    @QueryHints(value =
        @QueryHint(name = HINT_FETCH_SIZE, value = "" + Integer.MIN_VALUE)
    )
    @Query(value = "select u from User u where u.anotherId is null ")
    Stream<User> findAllByAnotherIdIsNull();
}

@SpringBootApplication
public class DemoJpaStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoJpaStreamApplication.class, args);
    }

}

@Configuration
@EnableJpaAuditing
class AppConfiguration {

    @Bean
    public AuditorAware<String> auditor() {
        return () -> "SYSTEM";
    }
}

@EntityListeners(AuditingEntityListener.class)
@MappedSuperclass
@Getter
@Setter
class AuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @CreatedDate
    private Instant creationDate;

    @CreatedBy
    private String createdBy;

    @LastModifiedDate
    private Instant lastModifiedDate;

    @LastModifiedBy
    private String lastModifiedBy;
}

@Entity
@Table(name = "user")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
class User extends AuditEntity {

    @Column
    private String name;

    @Column
    private String anotherId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o)) return false;
        User user = (User) o;

        return getId() != null && getId().equals(user.getId());
    }

    @Override
    public int hashCode() {
        return 562048007;
    }
}

@Service
@RequiredArgsConstructor
class Initiator {

    private static final int BATCH_SIZE = 100;

    private final UserService userService;

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        List<User> users = new ArrayList<>(BATCH_SIZE);
        IntStream.range(0, 50000)
            .forEach(value -> saveUser(users, value));
    }

    private void saveUser(List<User> users, int value) {
        User user = new User();
        user.setName("USER-" + value);
        users.add(user);

        if (BATCH_SIZE == users.size()) {
            userService.saveAll(users);

            users.clear();
        }
    }
}

@Service
@Slf4j
class UserService {

    private static final int BATCH_SIZE = 100;

    @Autowired
    private UserRepository userRepository;

    @Lazy
    @Autowired
    private UserService userServiceProxy;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveAll(List<User> users) {
        userRepository.save(users);
    }

    @Transactional(readOnly = true)
    public void updateAll() {
        try (Stream<User> users = userRepository.findAllByAnotherIdIsNull()) {
            List<User> userContainer = new ArrayList<>(BATCH_SIZE);
            users.forEach(user -> updateUser(userContainer, user));
        }
    }

    private void updateUser(List<User> userContainer, User user) {
        user.setAnotherId(UUID.randomUUID().toString());
        userContainer.add(user);
        log.info("Updated User Id: {}", user.getId());

        if (BATCH_SIZE == userContainer.size()) {
            flush(userContainer);
        }
    }

    private void flush(List<User> userContainer) {
        userServiceProxy.saveAll(userContainer);
        userContainer.clear();
        log.info("====FLUSHED====");
    }
}

@RestController
@RequiredArgsConstructor
class UserController {

    private final UserService userService;

    @GetMapping("/user")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void updateAll() {
        CompletableFuture.runAsync(userService::updateAll);
    }
}