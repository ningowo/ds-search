package team.dsys.dssearch.vo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class CommonResponse<T> implements Serializable {

    private int status;

    private String msg;

    private T data;

    public CommonResponse(int status) {
        this.status = status;
    }

    public CommonResponse(int status, T data) {
        this.status = status;
        this.data = data;
    }

    public CommonResponse(int status, String msg) {
        this.status = status;
        this.msg = msg;
    }

    public CommonResponse(int status, String msg, T data) {
        this.status = status;
        this.msg = msg;
        this.data = data;
    }

    public static <T> CommonResponse<T> createSuccessResult() {
        return new CommonResponse<T>(CommonResponseConts.SUCCESS, "ok");
    }

    public static <T> CommonResponse<T> createSuccessResult(T data) {
        return new CommonResponse<T>(CommonResponseConts.SUCCESS, "ok", data);
    }

    public static <T> CommonResponse<T> createFailResult() {
        return new CommonResponse<T>(CommonResponseConts.COMMON_ERROR, "Error, please contact developer for help.");
    }

    public static <T> CommonResponse<T> createFailResult(String msg) {
        return new CommonResponse<T>(CommonResponseConts.COMMON_ERROR, msg);
    }

}
