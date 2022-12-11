import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("1: Movie Rank Analyzer.");
        System.out.println("2: User Rank Analyzer.");
        System.out.println("3: Trend Analyzer.");
        System.out.println("Select analysis: ");
        Scanner scanner = new Scanner(System.in);
        String reportType = scanner.next();

        if (reportType.equals("1")){
            System.out.println("Running Movie Rank Report Analysis...");
            MovieRankAnalyzer.getReport();
        } else if (reportType.equals("2")) {
            System.out.println("Enter User Id: ");
            String userId = scanner.next();
            System.out.println("Running User Rank Analysis...");
            UserAnalyzer.getUserAnalyzer(userId);
        } else if (reportType.equals("3")) {
            System.out.println(
                "* Action\n* Adventure\n* Animation\n* Children's\n* Comedy\n* Crime\n* Documentary \n* Drama \n* Fantasy \n" +
                "* Film-Noir \n* Horror \n* Musical \n* Mystery \n* Romance \n* Sci-Fi \n* Thriller \n* War \n* Western");
            System.out.println("Enter A Genre: ");
            String trend = scanner.next();
            System.out.println("Running Trend Analysis...");
            GenreTrendAnalyzer.getReport(trend);
        }
    }
}
