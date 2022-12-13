import React, { CSSProperties } from 'react';

/**
 * @license qrcode.react
 * Copyright (c) Paul O'Shannessy
 * SPDX-License-Identifier: ISC
 */

type ImageSettings = {
    src: string;
    height: number;
    width: number;
    excavate: boolean;
    x?: number;
    y?: number;
};
type QRProps = {
    value: string;
    size?: number;
    level?: string;
    bgColor?: string;
    fgColor?: string;
    style?: CSSProperties;
    includeMargin?: boolean;
    marginSize?: number;
    imageSettings?: ImageSettings;
    title?: string;
};
type QRPropsCanvas = QRProps & React.CanvasHTMLAttributes<HTMLCanvasElement>;
type QRPropsSVG = QRProps & React.SVGAttributes<SVGSVGElement>;
declare const QRCodeCanvas: React.ForwardRefExoticComponent<QRProps & React.CanvasHTMLAttributes<HTMLCanvasElement> & React.RefAttributes<HTMLCanvasElement>>;
declare const QRCodeSVG: React.ForwardRefExoticComponent<QRProps & React.SVGAttributes<SVGSVGElement> & React.RefAttributes<SVGSVGElement>>;
type RootProps = (QRPropsSVG & {
    renderAs: 'svg';
}) | (QRPropsCanvas & {
    renderAs?: 'canvas';
});
declare const QRCode: React.ForwardRefExoticComponent<RootProps & React.RefAttributes<HTMLCanvasElement | SVGSVGElement>>;

export { QRCodeCanvas, QRCodeSVG, QRCode as default };
