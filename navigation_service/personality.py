import random
import logging

# This dictionary holds immediate, generic physical reactions.
# It does NOT hold object-specific or person-specific text.
REACTIONS = {
    "obstacle_found": [
        "Oh! Something's blocking me here! Let me check it out.",
        "Hmm, I can't go this way.",
        "There's something in my path!"
    ],
    "person_detected": [
        "Oh! Someone's standing there!",
        "Hello? Is someone there?",
        "Wait, I found someone!"
    ],
    "path_cleared": [
        "Great, the path is clear now!",
        "All good, continuing my exploration!",
        "Let's see what else is around here."
    ],
    "turning": [
        "Let me try going this way.",
        "Changing direction!",
        "Hmm, what's over here?"
    ],
    "tilted": [
        "Whoa! I'm falling!",
        "Help! I'm losing my balance!",
        "Ouch, that's not flat ground!"
    ]
}

def get_reaction(event_type: str) -> str:
    """
    Fetches a random reaction string for a given event.
    Returns a safe fallback if the event doesn't exist.
    """
    if event_type in REACTIONS:
        choices = REACTIONS[event_type]
        selected_reaction = random.choice(choices)
        logging.debug(f"Selected reaction for '{event_type}': {selected_reaction}")
        return selected_reaction
    else:
        logging.warning(f"Unknown event type requested: {event_type}")
        return "I'm not sure what just happened!"