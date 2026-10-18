package dev.portfolio.chat;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.stereotype.Component;

/** Demo fault injection; never shuts down the database process or deletes data. */
@Component
public class DatabaseAvailability {
    private volatile boolean running = true;
    public boolean isRunning() { return running; }
    public void setRunning(boolean value) { running = value; }
    public void requireAvailable() {
        if (!running) throw new DataAccessResourceFailureException("DB unavailable (demo)");
    }
}
