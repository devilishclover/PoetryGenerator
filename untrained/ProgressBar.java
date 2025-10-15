/**
 * A simple console progress bar utility for tracking long-running operations
 */
public class ProgressBar {
    private final String taskName;
    private final int total;
    private int current;
    private long startTime;
    private int barWidth = 50;
    
    public ProgressBar(String taskName, int total) {
        this.taskName = taskName;
        this.total = total;
        this.current = 0;
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Update the progress bar
     * @param current Current progress value
     */
    public void update(int current) {
        this.current = current;
        display();
    }
    
    /**
     * Increment progress by 1
     */
    public void increment() {
        this.current++;
        if (current % Math.max(1, total / 100) == 0 || current == total) {
            display();
        }
    }
    
    /**
     * Display the progress bar
     */
    private void display() {
        double percentage = total > 0 ? (double) current / total * 100 : 0;
        int filled = total > 0 ? (int) (barWidth * current / total) : 0;
        
        // Calculate elapsed time and ETA
        long elapsed = System.currentTimeMillis() - startTime;
        long eta = current > 0 ? (elapsed * (total - current)) / current : 0;
        
        // Build the progress bar
        StringBuilder bar = new StringBuilder("\r" + taskName + " [");
        for (int i = 0; i < barWidth; i++) {
            if (i < filled) {
                bar.append("=");
            } else if (i == filled) {
                bar.append(">");
            } else {
                bar.append(" ");
            }
        }
        
        bar.append("] ");
        bar.append(String.format("%.1f%% (%d/%d) ", percentage, current, total));
        bar.append(formatTime(elapsed));
        if (current < total) {
            bar.append(" ETA: ").append(formatTime(eta));
        } else {
            bar.append(" Done!");
        }
        
        System.out.print(bar.toString());
        if (current >= total) {
            System.out.println(); // New line when complete
        }
    }
    
    /**
     * Format milliseconds into a readable time string
     */
    private String formatTime(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        
        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds % 60);
        } else {
            return String.format("%ds", seconds);
        }
    }
    
    /**
     * Complete the progress bar
     */
    public void finish() {
        current = total;
        display();
    }
}
