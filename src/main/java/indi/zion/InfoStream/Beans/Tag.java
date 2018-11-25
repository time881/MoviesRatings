package indi.zion.InfoStream.Beans;

public class Tag {
    private int userID;
    private int movieID;
    private String tag;
    private int timeStamp;

    public Tag(int userID, int movieID, String tag, int timeStamp) {
        this.userID = userID;
        this.movieID = movieID;
        this.tag = tag;
        this.timeStamp = timeStamp;
    }

    public int getUserID() {
        return userID;
    }

    public int getMovieID() {
        return movieID;
    }

    public String getTag() {
        return tag;
    }

    public int getTimeStamp() {
        return timeStamp;
    }
}