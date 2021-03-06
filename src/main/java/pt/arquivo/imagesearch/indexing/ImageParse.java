package pt.arquivo.imagesearch.indexing;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;

import pt.arquivo.imagesearch.indexing.data.ImageData;
import org.apache.log4j.Logger;
import org.imgscalr.Scalr;
import org.imgscalr.Scalr.Method;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;


public class ImageParse {

    //TODO: extract this variables to config files
    public static final int THUMB_HEIGHT = 200;
    public static final int THUMB_WIDTH = 200;
    public static final int MIN_WIDTH = 51;
    public static final int MIN_HEIGHT = 51;

    public static final int MAX_WIDTH = 15000;
    public static final int MAX_HEIGHT = 15000;

    public static final String DIGEST_ALGORITHM = "SHA-256";

    public static Logger logger = Logger.getLogger(WARCInformationParser.class);

    /**
     *
     * @param img
     * @param widthThumbnail
     * @param heightThumbnail
     * @return
     * @throws InterruptedException
     */
//	public static ImageSearchResult getPropImage( String imageURL ) throws InterruptedException {
//		ImageSearchResult img = new ImageSearchResult( );
//		BufferedImage bimg;
//		String base64String, 
//			base64StringOriginal;	
//		String type = null;
//		URLConnection uc = null;
//		MessageDigest digest = null;
//		BufferedImage scaledImg = null;
//		ByteArrayOutputStream bao = new ByteArrayOutputStream( );
//		try {
//			int thumbWidth 	= 200, thumbHeight = 200;
//			uc = new URL( imageURL ).openConnection( );
//			uc.setConnectTimeout(10 * 1000);/*10 seconds*/
//			//uc.setRequestMethod("GET");
//			//uc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
//			uc.connect();
//			digest = MessageDigest.getInstance( "SHA-256" );
//			InputStream inImg =  uc.getInputStream( );
//			bimg = ImageIO.read( inImg );
//			type = getMimeType( uc );
//			int width          	= bimg.getWidth( null );
//			int height         	= bimg.getHeight( null );
//			img.setMime( type );
//			img.setHeight( Double.toString( height ) );
//			img.setWidth( Double.toString( width ) );
//			img.setUrl( imageURL );
//
//			byte[ ] bytesImgOriginal = IOUtils.toByteArray( new URL( imageURL ).openStream( ) );
//			//calculate digest
//			digest.update( bytesImgOriginal );
//			byte byteDigest[ ] = digest.digest();
//			img.setDigest( convertByteArrayToHexString( byteDigest ) );
//
//			if( width < thumbWidth || height < thumbHeight )
//				scaledImg = bimg;
//			else {
//				
//				if( type.equals( "image/gif" )  ) {
//					
//					//byte[] output = getThumbnailGif( inImg , thumbWidth , thumbHeight );
//					// Create a byte array output stream.
//					if( imageURL == null )
//						return null;
//			        
//					base64StringOriginal = Base64.encode( bytesImgOriginal );
//					bao.close( );
//					img.setThumbnail( base64StringOriginal );
//					return img;
//
//				} else {
//					double thumbRatio = (double) thumbWidth / (double) thumbHeight;
//					double imageRatio = (double) width / (double) height;
//					if ( thumbRatio < imageRatio ) 
//						thumbHeight = (int)( thumbWidth / imageRatio );
//					else 
//						thumbWidth = (int)( thumbHeight * imageRatio );
//					
//					if( width < thumbWidth && height < thumbHeight ) {
//						thumbWidth  = width;
//						thumbHeight = height;
//					} else if( width < thumbWidth )
//						thumbWidth = width;
//					else if( height < thumbHeight )
//						thumbHeight = height;
//					
//					scaledImg = Scalr.resize( bimg, 
//												Method.QUALITY, 
//												Scalr.Mode.AUTOMATIC, 
//												thumbWidth, 
//												thumbHeight, 
//												Scalr.OP_ANTIALIAS ); //create thumbnail				
//				}
//			}
//
//
//			// Write to output stream
//	        ImageIO.write( scaledImg , type.substring( 6 ) , bao );
//	        bao.flush( );
//	        
//	        // Create a byte array output stream.
//	        base64String = Base64.encode( bao.toByteArray( ) );
//	        bao.close( );
//	        img.setThumbnail( base64String );
//	
//			return img;
//
//		} catch ( MalformedURLException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch ( IOException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch( IllegalArgumentException e ) {
//			e.printStackTrace( );
//			return null;
//		} catch ( Exception e ) {
//			 e.printStackTrace();
//			 return null;
//		}
//		
//	}
    

    /**
     * convert the byte to hex format method (digest imgge)
     *
     * @param byteData
     * @return
     */
    private static String convertByteArrayToHexString(byte[] byteData) {
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            String hex = Integer.toHexString(0xff & byteData[i]);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }

        return hexString.toString();
    }

    public static ImageData getPropImage(ImageData img) {

        try {
            MessageDigest digest = null;
            BufferedImage scaledImg = null;

            Map.Entry<ImageReader, Dimension> data = WARCInformationParser.getImageDimensions(img);

            //error getting dimensions; probably invalid or unsupported image type
            if (data == null)
                return null;


            ImageReader reader = data.getKey();
            Dimension imgDimensions = data.getValue();

            int width = imgDimensions.width;
            int height = imgDimensions.height;

            img.setHeight(height);
            img.setWidth(width);

            //To not do further processing, it will be ignored at the next stage
            //This is only returning a correct value to collect statistics in the map job
            if (width < MIN_WIDTH || height < MIN_HEIGHT || (height * width > MAX_HEIGHT * MAX_WIDTH))
                return img;

            digest = MessageDigest.getInstance(DIGEST_ALGORITHM);

            byte[] bytesImgOriginal = img.getBytes();

            //calculate digest
            digest.update(bytesImgOriginal);
            byte[] byteDigest = digest.digest();
            String stringDigest = convertByteArrayToHexString(byteDigest);
            img.addContentHash(stringDigest);

            if (img.getUrl().startsWith("data:image")){
                img.setUrl(stringDigest);
                img.setSurt(stringDigest);
            }

            // avoid reading gifs or svgs, as they do not need be resized
            if (img.getMimeDetected().equals("image/gif") || img.getMimeDetected().contains("svg")) {
                img.setBytes(bytesImgOriginal);
                return img;
            }

            BufferedImage bimg;

            ImageReadParam param = reader.getDefaultReadParam();

            try {
                bimg = reader.read(reader.getMinIndex(), param);
            } finally {
                reader.dispose();
            }

            if (width < THUMB_WIDTH || height < THUMB_HEIGHT) {
                scaledImg = bimg;
            } else {

                int thumbWidth = THUMB_WIDTH, thumbHeight = THUMB_HEIGHT;
                double thumbRatio = (double) thumbWidth / (double) thumbHeight;
                double imageRatio = (double) width / (double) height;
                if (thumbRatio < imageRatio)
                    thumbHeight = (int) (thumbWidth / imageRatio);
                else
                    thumbWidth = (int) (thumbHeight * imageRatio);

                if (width < thumbWidth)
                    thumbWidth = width;
                else if (height < thumbHeight)
                    thumbHeight = height;

                scaledImg = Scalr.resize(bimg,
                        Method.AUTOMATIC,
                        Scalr.Mode.AUTOMATIC,
                        thumbWidth,
                        thumbHeight,
                        Scalr.OP_ANTIALIAS); //create thumbnail
            }

            ByteArrayOutputStream bao = new ByteArrayOutputStream();

            // Write to output stream
            ImageIO.write(scaledImg, img.getMimeDetected().substring(6), bao);
            bao.flush();

            // Create a byte array output stream.
            img.setBytes(bao.toByteArray());
            bao.close();


            return img;

        } catch (
                NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (
                IOException e) {
            logger.error(String.format("Error loading image url: %s with error message %s", img.getURLWithTimestamp(), e.getMessage()));
        }
        return null;
    }

}

