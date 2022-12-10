import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        System.out.println("1: Movie Rank Analyzer.");
        System.out.println("2: User Rank Analyzer.");
        System.out.println("Select analysis: ");
        Scanner scanner = new Scanner(System.in);
        String reportType = scanner.next();

        if (reportType.equals("1")){
            System.out.println("Movie Rank Report Selected");
            MovieRankAnalyzer.getReport();
        } else if (reportType.equals("2")) {
            System.out.println("User Rank Analyzer");
            System.out.println("Enter User Id: ");
            String userId = scanner.next();
            UserAnalyzer.getUserAnalyzer(userId);
        }
    }
}
