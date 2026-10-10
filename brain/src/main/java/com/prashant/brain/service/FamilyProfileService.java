package com.prashant.brain.service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
@Service
@Slf4j
public class FamilyProfileService {
    private final StringRedisTemplate redisTemplate;
    public FamilyProfileService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    private static final String profileHashKey= "robot:profiles";
    public void saveProfile(String name , String customGreeting){
        redisTemplate.opsForHash().put(profileHashKey,name.toLowerCase(),customGreeting);
        log.info("Saved profile for {} with greeting: {}", name, customGreeting);
    }
    public String getGreetingForPerson(String name){
        String greeting = (String) redisTemplate.opsForHash().get(profileHashKey,name.toLowerCase());
        log.info("Retrieved greeting for {}: {}", name, greeting);
        return (greeting == null)? "Hello," + name + "i think we haven't met" : greeting;
    }
}
