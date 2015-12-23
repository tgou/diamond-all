package com.taobao.diamond.domain;

/**
 * @author leiwen.zh
 * 
 */
public class ConfigInfoEx extends ConfigInfo {

    private static final long serialVersionUID = -1L;

    // Batch query, see Constants
    private int status;
    // error message
    private String message;


    public ConfigInfoEx() {
        super();
    }


    public ConfigInfoEx(String dataId, String group, String content) {
        super(dataId, group, content);
    }


    public int getStatus() {
        return status;
    }


    public void setStatus(int status) {
        this.status = status;
    }


    public String getMessage() {
        return message;
    }


    public void setMessage(String message) {
        this.message = message;
    }


	@Override
	public String toString() {
		return "ConfigInfoEx [status=" + status + ", message=" + message
				+ ", getDataId()=" + getDataId() + ", getGroup()=" + getGroup()
				+ ", getContent()=" + getContent() + ", getMd5()=" + getMd5()
				+ "]";
	}

}
