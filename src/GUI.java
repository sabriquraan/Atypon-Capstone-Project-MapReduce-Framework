import javafx.application.Application;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class GUI extends Application {

    public static void main(String[] args) {
        launch(args);
    }
    @Override
    public void start(Stage primaryStage) {

        HomeScreen homeScreen = new HomeScreen();
        Scene scene = homeScreen.getScreen();
        if (scene==null){
            System.out.println("Error in home screen\n");
            return;
        }

        primaryStage.setScene(scene);
        primaryStage.setTitle("MapReduce System");
        primaryStage.show();

    }
}

