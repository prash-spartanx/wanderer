package com.prashant.brain.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@Slf4j
public class ObjectResponse {
    private final StringRedisTemplate redisTemplate;
    private final Random random = new Random();

    public ObjectResponse(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public String GenerateReaction(String object) {
        if (object == null || object.isBlank()) {
            return "Hmm… I don’t see anything right now!";
        }

        // Some innocent childlike reactions
        String[] reactions = {
                "Oh wow, I see a %s! It looks so fun!",
                "Hehe, that’s a %s… I wonder what it does!",
                "Yay, a %s! Can I play with it?",
                "Ooooh, a %s… it makes me curious!",
                "Oh I see a %s, and it feels magical!"
        };

        String template = reactions[random.nextInt(reactions.length)];
        String reaction = String.format(template, object);

        log.info("Generated reaction for object '{}': {}", object, reaction);

        // Optional: store last reaction in Redis
        redisTemplate.opsForValue().set("lastReaction:" + object, reaction);

        return reaction;
    }
}
