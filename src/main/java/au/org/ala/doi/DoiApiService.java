// IntelliJ API Decompiler stub source generated from a class file
// Implementation of methods is not available
package au.org.ala.doi;

import okhttp3.MultipartBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

public interface DoiApiService {
    @Headers("Accept-Version: 1.0")
    @POST("doi")
    Call<CreateDoiResponse>  create(
            @retrofit2.http.Body CreateDoiRequest createDoi );

    @Headers("Accept-Version: 1.0")
    @Multipart
    @POST("doi")
    Call<CreateDoiResponse> createMultipart(
            @Part MultipartBody.Part file , @Part("json") CreateDoiRequest createDoi );

    @Headers("Accept-Version: 1.0")
    @Multipart
    @POST("doi/{doi}")
    Call<Doi> update(@Path(value = "doi", encoded = false) String doi, @Part("json") UpdateDoiRequest updateDoi );

    @Headers("Accept-Version: 1.0")
    @Multipart
    @POST("doi/{doi}")
    Call<Doi> updateMultipart(@Path(value = "doi", encoded = false)  String doi,
            @Part MultipartBody.Part file, @Part("json") UpdateDoiRequest updateDoi);

    @Headers("Accept-Version: 1.0")
    @GET("doi/{doi}")
    Call<Doi> get(@Path(value = "doi", encoded = false) String doi);

    @Headers("Accept-Version: 1.0")
    @GET("doi/{doi}")
    Call<Doi> getEncoded(@Path(value = "doi", encoded = true) String doi);

    @Headers("Accept-Version: 1.0")
    @GET("doi/{doi}/download")
    Call<ResponseBody> download(@Path(value = "doi", encoded = false) String doi);

    @Headers("Accept-Version: 1.0")
    @GET("doi/{doi}/download")
    Call<ResponseBody> downloadEncoded(@Path(value = "doi", encoded = true) String doi);

}

